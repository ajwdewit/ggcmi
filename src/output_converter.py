import conv_settings
import sys
sys.path.append(conv_settings.pcse_dir)
import logging
from sqlalchemy.exc import SQLAlchemyError
from crop_sim_output_worker import CropSimOutputWorker
from joint_netcdf4_raster import JointNetcdf4Raster
from joint_shelves import JointShelves
from numpy import frompyfunc
from pcse.util import doy
from multiprocessing import Pool, Process

# Constants
nrows = 360
ncols = 720
xll = -180.0
yll = 90.0
cellsize = 0.5
nodatavalue = 1.e+20

# Define some lambda functions to take care of unit conversions.
no_conv = frompyfunc(lambda x: x, 1, 1)
cm_day_to_mm_day1 = frompyfunc(lambda x: 10*x if x!=None else nodatavalue, 1, 1)
cm_day_to_mm_day2 = frompyfunc(lambda x, y: 10*(x+y) if x!=None else 10*y, 2, 1)
cm_day_to_mm_day3 = frompyfunc(lambda x, y, z: 10*x*(y-z).days if x!=None and y!=None and z!=None else nodatavalue, 3, 1)
kg_ha_to_t_ha = frompyfunc(lambda x: x*0.001 if x!=None else nodatavalue, 1, 1)
date_to_doy = frompyfunc(doy, 1, 1)
date_to_days_since_planting2 = frompyfunc(lambda x, y: (x-y).days if x!=None and y!=None else nodatavalue, 2, 1)
local_conv = frompyfunc(lambda w, x, y, z: w+x+y+z, 4, 1)

variables = {"yield":     ("Crop yield (dry matter) :: t ha-1 yr-1", "TWSO", kg_ha_to_t_ha),
             "pirrww":    ("Applied irrigation water :: mm yr-1", "", no_conv),
             "biom":      ("Total Above ground biomass yield :: t ha-1 yr-1", "TAGP", kg_ha_to_t_ha),
             "aet":       ("Actual growing season evapotranspiration :: mm yr-1", "EVST,CTRAT", cm_day_to_mm_day2),
             "plant-day": ("Actual planting date :: day of year", "DOS", date_to_doy),
             "anth-day":  ("Days from planting to anthesis :: days", "DOA,DOS", date_to_days_since_planting2),
             "maty-day":  ("Days from planting to maturity :: days", "DOH,DOM,DOS,task_id", local_conv),
             "initr":     ("Nitrogen application rate :: kg ha-1 yr-1", "", no_conv),
             "leach":     ("Nitrogen leached :: kg ha-1 yr-1", "", no_conv),
             "sco2":      ("Soil carbon emissions :: kg C ha-1", "", no_conv),
             "sn2o":      ("Nitrous oxide emissions :: kg N2O-N ha-1", "", no_conv),
             "gsprcp":    ("Accumulated precipitation, planting to harvest :: mm yr-1", "", cm_day_to_mm_day1), #GSRAINSUM
             "gsrsds":    ("Growing season incoming solar :: w m-2 yr-1" , "", cm_day_to_mm_day1), #GSRADIATIONSUM
             "smt":       ("Sum of daily mean temps, planting to harvest :: deg C-days yr-1", "", cm_day_to_mm_day3) #GSTEMPAVG
            }

def f(x):
    # Filter out None values
    if x == None:
        return nodatavalue
    else:
        return x

class OutputConverter():
    # Initialise
    _joint_netcdf4 = None
    _worker = None
    _joint_shelves = None
    _start_year = 1945
    _end_year = 2045

    def __init__(self, crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year): 
        # Prepare a suitable input structure
        print "About to open shelves with simulation output ..."
        self._joint_shelves = JointShelves(conv_settings.shelve_folder) 

        # Derive labels from table cropinfo
        worker = CropSimOutputWorker(crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year)  
        worker.set_joint_shelves(self._joint_shelves)
        msg ="About to retrieve simulation results for crop %s (%s)"        
        print msg % (worker._crop_label, worker._mgmt_code)

        # Make sure only files are opened for the relevant variables
        rasterkeys = []
        for var in variables.keys():
            if variables[var][1] != "": rasterkeys.append(var)

        # Prepare the output files    
        path2template = worker._get_path_to_template();
        ncdf_pattern = worker._get_output_filename_pattern()
        self._joint_netcdf4 = JointNetcdf4Raster(path2template, conv_settings.results_folder, ncdf_pattern, rasterkeys)
            
    def close(self):
        # Close all objects for this crop
        if self._joint_netcdf4 != None:
            self._joint_netcdf4.close()
            msg = "Finished writing output in netcdf4 format for crop %s (%s)"
            print msg % (self._worker._crop_label, self._worker._mgmt_code)
        if self._worker != None:
            self._worker.close()
            self._worker = None
        if self._joint_shelves != None:
            self._joint_shelves.close()
            self._joint_shelves = None
            del self._joint_shelves
            
    def run(self):
        worker = self._worker
        try:
            # All output files should be opened now 
            if not self._joint_netcdf4.open('a', worker._start_year, worker._end_year, ncols, nrows, xll, yll, cellsize, nodatavalue):
                raise RuntimeError()
            
            # Make sure that attributes are given the right names
            msg = "About to write output in netcdf4 format for crop %s (%s)"
            print msg % (worker._crop_label, worker._mgmt_code)
            for var in variables:
                # In case of yield, the crop_label has to be added
                cvt = variables[var]
                if cvt[1] == "": continue
                parts = cvt[0].split("::")
                name = var + "_" + worker._crop_label
                self._joint_netcdf4.writeheader(var, name, parts[0].strip(), parts[1].strip())
             
            # For each task, prepare a dictionary with relevant output
            rows = worker._get_finished_tasks(worker._crop_no)
            for task_id in rows:
                # Convert WOFOST output to the desired output format
                print "About to retrieve output from task %s" % task_id
                simresult = worker._get_simresult(task_id)
                if not simresult: continue 
                lon = simresult["longitude"]
                lat = simresult["latitude"]
                allresults = simresult["allresults"]
                values = {}
                
                # For WOFOST, the first campaign year is the year during which the harvest occurred
                # To register the result for the right year here, we have to start counting at DOS 
                # We start simulating @days_before_CROP_START_DATE. If that's before 1/1/start_year 
                # then we'll not find a DOS during the start_year: no simulation for that start_year!
                firstyear = allresults[0]["year"] # WOFOST
                yrcount = 0
                if (firstyear != worker._start_year): yrcount = 1
                prevyear = int(firstyear) - 1
                for record in allresults: 
                    # Check whether this is really the next year
                    curyear = int(record["year"])
                    if (curyear - prevyear > 1):
                        yrcount += (curyear - prevyear - 1)
                    summary = record["summary"]
                    if type(summary) is list:
                        summary = summary[0]
                    for var in variables:
                        # Get the conversion table (cvt) and function (conv)
                        cvt = variables[var]
                        if cvt[1] == "": continue
                        conv = cvt[2]
                        if conv.nin == 1:
                            # Just convert the value found in the dictionary
                            values[var] = f(conv(summary[cvt[1]]))
                        else:
                            # Assume there are 2 or 3 input arguments to deal with
                            try: 
                                argnames = cvt[1].split(',')
                                args = []
                                for name in argnames:
                                    if name in summary.keys():
                                        args.append(summary[name])
                                    else:
                                        args.append(None)
                                if len(argnames) == 2:
                                    values[var] = f(conv(args[0], args[1]))
                                elif len(argnames) == 3: 
                                    values[var] = f(conv(args[0], args[1], args[2]))
                                else:
                                    if var == "maty-day":
                                        doh = summary["DOH"]
                                        dom = summary["DOM"]
                                        dos = summary["DOS"]
                                        values[var] = f(worker._get_length_of_season(doh, dom, dos, task_id))
                            except:
                                continue
                     
                    # After filling the values for this year, assign them to the output raster!           
                    self._joint_netcdf4.set_data(yrcount, lon, lat, values)
                    yrcount = yrcount + 1
                    prevyear = curyear

            # Now write
            print "Stored values will now be written to the netCDF4 files ..."
            for _ in range(nrows):
                self._joint_netcdf4.writenext()

        except SQLAlchemyError:
            msg = "Database error on crop %i." % worker._crop_no
            print msg
            logging.exception(msg)
        except RuntimeError:
            msg = "Error opening netCDF4 dataset on crop %i." % worker._crop_no
            print msg
            logging.exception(msg)
        finally:
            if worker != None:
                worker.close()
                worker = None
                
def convert_to_nc4(crop_no):
    # Constants needed to write output files
    model = "cgms-wofost"
    climate = "WFDEI"
    start_year = 1979
    end_year = 2012
    clim_scenario = "hist"
    sim_scenario = "default"
    
    try:
        converter = OutputConverter(crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year)
        converter.run()
        converter.close()
    except KeyboardInterrupt:
        msg = "While working on data for crop-management combination no. %s, a user request was received. Quitting ..." 
        print msg % crop_no
        logging.error(msg)
        sys.exit()
        
def main():
    p = Pool(3)
    crops = range(1, 29)
    p.map(convert_to_nc4, crops)
    # p= Process(target=convert_to_nc4, args=(1,))
    # p.start()
    # p.join()

if (__name__ == "__main__"):
    main()
        

