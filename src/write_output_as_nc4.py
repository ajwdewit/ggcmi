import run_settings
import sys
sys.path.append(run_settings.pcse_dir)
import logging, os
from numpy import frompyfunc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import engine as sa_engine
from joint_netcdf4_raster import JointNetcdf4Raster
from joint_shelves import JointShelves
from pcse.util import doy
from cropinforeader import CropInfoProvider

# Define some lambda functions to take care of unit conversions.
no_conv = frompyfunc(lambda x: x, 1, 1)
cm_day_to_mm_day1 = frompyfunc(lambda x: 10*x if x!=None else None, 1, 1)
cm_day_to_mm_day2 = frompyfunc(lambda x, y: 10*(x+y) if x!=None else 10*y, 2, 1)
cm_day_to_mm_day3 = frompyfunc(lambda x, y, z: 10*x*(y-z).days if x!=None and y!=None and z!=None else None, 3, 1)
kg_ha_to_t_ha = frompyfunc(lambda x: x*0.001 if x!=None else None, 1, 1)
date_to_doy = frompyfunc(doy, 1, 1)
date_to_days_since_planting2 = frompyfunc(lambda x, y: (x-y).days if x!=None and y!=None else None, 2, 1)
local_conv = frompyfunc(lambda w, x, y, z: w+x+y+z, 4, 1)

variables = {"yield":     ("Crop yield (dry matter) :: t ha-1 yr-1", "TWSO", kg_ha_to_t_ha),
             "pirrww":    ("Applied irrigation water :: mm yr-1", "", no_conv),
             "biom":      ("Total Above ground biomass yield :: t ha-1 yr-1", "TAGP", kg_ha_to_t_ha),
             "aet":       ("Actual growing season evapotranspiration :: mm yr-1", "EVST,CTRAT", cm_day_to_mm_day2),
             "plant_day": ("Actual planting date :: day of year", "DOS", date_to_doy),
             "anth_day":  ("Days from planting to anthesis :: days", "DOA,DOS", date_to_days_since_planting2),
             "maty_day":  ("Days from planting to maturity :: days", "DOH,DOM,DOS,task_id", local_conv),
             "initr":     ("Nitrogen application rate :: kg ha-1 yr-1", "", no_conv),
             "leach":     ("Nitrogen leached :: kg ha-1 yr-1", "", no_conv),
             "sco2":      ("Soil carbon emissions :: kg C ha-1", "", no_conv),
             "sn2o":      ("Nitrous oxide emissions :: kg N2O-N ha-1", "", no_conv),
             "gsprcp":    ("Accumulated precipitation, planting to harvest :: mm yr-1", "", cm_day_to_mm_day1), #GSRAINSUM
             "gsrsds":    ("Growing season incoming solar :: w m-2 yr-1" , "", cm_day_to_mm_day1), #GSRADIATIONSUM
             "smt":       ("Sum of daily mean temps, planting to harvest :: deg C-days yr-1", "", cm_day_to_mm_day3) #GSTEMPAVG
            }

class CropSimOutputWorker():
    _crop_no = 0
    _crop_name = ""
    _crop_label = ""
    _mgmt_code = ""
    _model = ""
    _climate = ""
    _clim_scenario = ""
    _sim_scenario = ""
    _timestep = "annual"
    _start_year = 1945
    _end_year = 2045
    _db_engine = None
    _crop_info_provider = None 
    _joint_shelves = None
    
    def __init__(self, crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year):
        # Initialise
        self._crop_no = crop_no
        self._model = model
        self._climate = climate
        self._clim_scenario = clim_scenario
        self._sim_scenario = sim_scenario
        self._start_year = start_year
        self._end_year = end_year 
        try:
            self._db_engine = sa_engine.create_engine(run_settings.connstr) 
            
            # Retrieve some info about the crop
            self._crop_name, self._crop_label, self._mgmt_code = self._get_crop_info(crop_no)
            self._crop_info_provider = CropInfoProvider(self._crop_name, self._mgmt_code)
            
        except Exception as e:
            print " Error during initialisation of CropSimOutputWorker instance: \n" + str(e)
            raise e
        
    def set_joint_shelves(self, obj):
        if isinstance(obj, JointShelves):
            self._joint_shelves = obj;
        
    def _get_path_to_template(self):
        template_fn = "output_template_{clim_lc}_{timestep}_{start_year}_{end_year}.nc4"
        template_fn = template_fn.format(clim_lc=self._climate.lower(), timestep=self._timestep,
                                         start_year=self._start_year, end_year=self._end_year)
        return os.path.join(run_settings.results_folder, template_fn)

    def _get_output_filename_pattern(self):
        ncdfname_templ = "{model}_{climate}_{clim_scenario}_{sim_scenario}_{variable}_{crop}_{timestep}_{start_year}_{end_year}.nc4"
        if self._mgmt_code == 'rf': simcode = self._sim_scenario + "_noirr"
        else: simcode = self._sim_scenario + "_firr"
        result = ncdfname_templ.format(model=self._model, climate=self._climate.lower(), clim_scenario=self._clim_scenario,
                                       sim_scenario=simcode, variable="*", crop=self._crop_label,
                                       timestep=self._timestep, start_year=self._start_year, end_year=self._end_year)
        return result
    
    def _get_length_of_season(self, doh, dom, dos, task_id):
        if doh or dom:
            # doh or dom is specified - assume date objects
            f = lambda x, y, z: (x-z).days if x else (y-z).days
            return f(doh, dom, dos) 
        else:
            crop_no, lon, lat = self._get_task_info(task_id)
            if crop_no != self._crop_no:
                raise ValueError("The crop number found for task %s is not as expected" % task_id)
            start_doy, end_doy = self._crop_info_provider.getSeasonDates(float(lon), float(lat))
            timerdata = self._crop_info_provider.getTimerData(start_doy, end_doy, dos.year)
            result = (timerdata['CROP_END_DATE'] - dos).days
            if result < 0: result = result + 365
            return result
        
    def close(self):
        self._db_engine = None
        self._joint_shelves = None
    
    def _get_crop_info(self, crop_no):
        crop_name = ""
        crop_label = ""
        mgmt_code = ""
        conn = self._db_engine.connect()
        sqlStr = """SELECT m.crop_no, x.name, x.label, m.mgmt_code 
                 FROM crop m INNER JOIN cropinfo x ON m.crop_no = x.crop_no
                 WHERE m.crop_no=%s"""
        rows = conn.execute(sqlStr % crop_no)
        for row in rows:
            crop_name = row["name"]
            crop_label = row["label"]
            mgmt_code = row["mgmt_code"]
            break
        conn.close()
        return crop_name, crop_label, mgmt_code

    def _get_task_info(self, taskId):
        crop_no = 0
        lon = 180.0
        lat = 90.0
        conn = self._db_engine.connect()
        sqlStr = """select t.crop_no, t.longitude, t.latitude
                 from tasklist t where t.task_id =""" + str(taskId)
        rows = conn.execute(sqlStr)
        for row in rows:
            crop_no = row["crop_no"]
            lon = row["longitude"]
            lat = row["latitude"]
            break
        conn.close()
        return crop_no, lon, lat
    
    def _get_finished_tasks(self, crop_no):
        result = []
        conn = self._db_engine.connect()
        sqlStr = """SELECT task_id FROM tasklist
                 WHERE crop_no=%s AND status='Finished'"""
        rows = conn.execute(sqlStr % crop_no)
        conn.close()
        for row in rows:
            result.append(row["task_id"])
        return result
    
    def _get_simresult(self, task_id):
        try:
            key_fmt = "%010i"
            return self._joint_shelves[key_fmt % task_id]
        except KeyError:
            msg = "No results found for task_id %s " % task_id
            print msg
            logging.warn(msg)
            return None
        
def main():
    # Constants
    nrows = 360
    ncols = 720
    xll = -180.0
    yll = 90.0
    cellsize = 0.5
    nodatavalue = 1.e+20
    
    # Constants needed to write output files
    model = "cgms"
    climate = "AgMERRA"
    start_year = 1980
    end_year = 2010
    clim_scenario = "hist"
    sim_scenario = "default"
    
    # Initialise
    joint_netcdf4 = None
    worker = None
    joint_shelves = None
    
    # Make sure only relevant files are opened
    rasterkeys = []
    for var in variables.keys():
        if variables[var][1] != "": rasterkeys.append(var)
        
    try:
        # Prepare a suitable input structure
        print "About to open shelves with simulation output ..."
        joint_shelves = JointShelves(run_settings.shelve_folder) 
        
        for crop_no in range(11,29):
            # Derive labels from table cropinfo
            worker = CropSimOutputWorker(crop_no, model, climate, clim_scenario, sim_scenario, start_year, end_year)  
            worker.set_joint_shelves(joint_shelves)
            msg ="About to retrieve simulation results for crop %s (%s)"        
            print msg % (worker._crop_label, worker._mgmt_code)
                
            # Prepare the output files and open them    
            path2template = worker._get_path_to_template();
            ncdf_pattern = worker._get_output_filename_pattern()
            joint_netcdf4 = JointNetcdf4Raster(path2template, run_settings.results_folder, ncdf_pattern, rasterkeys)
            if not joint_netcdf4.open('a', start_year, end_year, ncols, nrows, xll, yll, cellsize, nodatavalue):
                continue
            
            # Make sure that attributes are given the right names
            msg = "About to write output in netcdf4 format for crop %s (%s)"
            print msg % (worker._crop_label, worker._mgmt_code)
            for var in variables:
                # In case of yield, the crop_label has to be added
                cvt = variables[var]
                if cvt[1] == "": continue
                parts = cvt[0].split("::")
                if var == "yield": name = var + "_" + worker._crop_label
                else: name = var
                joint_netcdf4.writeheader(var, name, parts[0].strip(), parts[1].strip())
             
            # For each task, prepare a dictionary with relevant output
            rows = worker._get_finished_tasks(crop_no)
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
                for record in allresults: 
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
                            values[var] = conv(summary[cvt[1]])
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
                                    values[var] = conv(args[0], args[1])
                                elif len(argnames) == 3: 
                                    values[var] = conv(args[0], args[1], args[2])
                                else:
                                    if var == "maty_day":
                                        doh = summary["DOH"]
                                        dom = summary["DOM"]
                                        dos = summary["DOS"]
                                        values[var] = worker._get_length_of_season(doh, dom, dos, task_id)
                            except:
                                continue
                     
                    # After filling the values for this year, assign them to the output raster!           
                    joint_netcdf4.set_data(yrcount, lon, lat, values)
                    yrcount = yrcount + 1

            # Now write
            print "Stored values will now be written to the netCDF4 files ..."
            for _ in range(nrows):
                joint_netcdf4.writenext()
        
            # Close all files for this crop
            joint_netcdf4.close()
            msg = "Finished writing output in netcdf4 format for crop %s (%s)"
            print msg % (worker._crop_label, worker._mgmt_code)
            worker.close()
            worker = None
    except SQLAlchemyError as inst:
        msg = "Database error on crop %i." % crop_no
        print msg
        logging.exception(msg)
    except RuntimeError as inst:
        msg = "Error opening netCDF4 dataset on crop %i." % crop_no
        print msg
        logging.exception(msg)
    except KeyboardInterrupt:
        msg = "While working on data about %s (%s), a user request was received. Quitting ..." 
        print msg % (worker._crop_label, worker._mgmt_code)
        logging.error(msg)
        sys.exit()
    finally:
        # Clean up
        if joint_shelves != None:
            joint_shelves.close()
            joint_shelves = None
            del joint_shelves

if (__name__ == "__main__"):
    main()

    
"""
def parse_expression(s, summary):
    # First detect operator - expect only one
    operator = ""
    if ('+' in s):
        operator = '+'
    else if ('*' in s):
        operator = '*'
    vars = s.split(operator)
    expr = str(summary[vars[0]]) + operator + str(summary[vars[1]])
    return eval(expr)
"""