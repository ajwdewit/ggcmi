import run_settings
import sys
sys.path.append(run_settings.pcse_dir)
from pcse.exceptions import PCSEError
import logging, os
from numpy import frompyfunc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import engine as sa_engine
from joint_netcdf4_raster import JointNetcdf4Raster
from joint_shelves import JointShelves
from pcse.util import doy

# Define some lambda functions to take care of unit conversions.
no_conv = frompyfunc(lambda x: x, 1, 1)
cm_day_to_mm_day1 = frompyfunc(lambda x: 10*x, 1, 1)
cm_day_to_mm_day2 = frompyfunc(lambda x, y: 10*(x + y), 2, 1)
cm_day_to_mm_day3 = frompyfunc(lambda x, y: 10* x * y, 2, 1)
kg_ha_to_t_ha = frompyfunc(lambda x: 1000*x, 1, 1)
date_to_doy = frompyfunc(doy, 1, 1)
date_to_days_since_planting = frompyfunc(lambda x, y: (y-x).days, 2, 1)

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

# TODO: Use DOH if DOM is not valid!
variables = {"yield":     ("Crop yield (dry matter) in t ha-1 yr-1", "TWSO", kg_ha_to_t_ha),
             "pirrww":    ("Applied irrigation water in mm yr-1", "", no_conv),
             "biom":      ("Total Above ground biomass yield in t ha-1 yr-1", "TAGP" kg_ha_to_t_ha),
             "aet":       ("Actual growing season evapotranspiration in mm yr-1", "EVST+CTRAT", cm_day_to_mm_day2),
             "plant_day": ("Actual planting date in day of year", "DOS", date_to_doy),
             "anth_day":  ("Days from planting to anthesis in days", "DOA-DOS", date_to_days_since_planting),
             "maty_day":  ("Days from planting to maturity in days", "DOM-DOS", date_to_days_since_planting),
             "initr":     ("Nitrogen application rate in kg ha-1 yr-1", "", no_conv),
             "leach":     ("Nitrogen leached in kg ha-1 yr-1", "", no_conv),
             "sco2":      ("Soil carbon emissions in kg C ha-1", "", no_conv),
             "sn2o":      ("Nitrous oxide emissions in kg N2O-N ha-1", "", no_conv),
             "gsprcp":    ("Accumulated precipitation, planting to harvest in mm yr-1", "GSRAINSUM", cm_day_to_mm_day),
             "gsrsds":    ("Growing season incoming solar in w m-2 yr-1" , "GSRADIATIONSUM", cm_day_to_mm_day),
             "smt":       ("Sum of daily mean temps, planting to harvest in deg C-days yr-1", "GSTEMPAVG*ndays", cm_day_to_mm_day3)
            }
 
timestep = "weekly"
ncdfname_templ = "{model}_{climate}_{clim_scenario}_{sim_scenario}_{variable}_{crop}_{timestep}_{start_year}_{end_year}.nc4"

def main():
    # Open database connection and empty output table
    db_engine = sa_engine.create_engine(run_settings.connstr)
    crop_no = 0
    
    try:
        for crop_no in range(1,29):
            # Prepare files for this crop - labels to be derived from table cropinfo
            crop_label, mgmt_code = get_crop_info(db_engine, crop_no)
            for var in variables:
                if mgmt_code == 'rf': simcode = sim_scenario + "_noirr"
                else: simcode = sim_scenario + "_firr"
                
                ncdfname = ncdfname_templ.format(model=model, 
                                                 climate=climate,
                                                 clim_scenario=clim_scenario,
                                                 sim_scenario=simcode,
                                                 variable="*",
                                                 crop=crop_label,
                                                 timestep=timestep,
                                                 start_year=start_year,
                                                 end_year=end_year)
            
            # Prepare a suitable input structure
            joint_shelves = JointShelves(run_settings.shelve_folder)   
                
            # Prepare a suitable output structure
            template_fn = "{climate}\output_template_{clim_lc}_annual_{start_year}_{end_year}.nc4"
            template_fn = template_fn.format(climate=climate, clim_lc=climate.lower(), start_year=start_year, end_year=end_year)
            path2template = os.path.join(run_settings.data_dir, template_fn)
            joint_netcdf4 = JointNetcdf4Raster(path2template, fpath=run_settings.output_folder, ncdfname, variables.keys())
            if not joint_netcdf4.open('w', start_year, end_year, ncols, nrows, xll, yll, cellsize, nodatavalue):
                continue
            
            # Make sure that attributes are given the right names
            for var in variables:
                # In case of yield, the crop_label has to be added
                cvt = variables[var]
                parts = cvt[0].split("in")
                if var == "yield": var = var + "_" + crop_label
                joint_netcdf4.writeheader(var, parts[0].strip(), parts[1])
            
            # Retrieve all the output for this crop
            rows = get_finished_tasks(db_engine, crop_no)
            for task_id in rows:
                simresult = joint_shelves.__getitem__(task_id)
                lon = simresult["longitude"]
                lat = simresult["latitude"]
                
                # Convert WOFOST output to the desired output format
                allresults = simresult["allresults"]
                values = {}
                for record in allresults: 
                    year = record["year"]
                    summary = record["summary"]
                    for var in variables:
                        # Get the conversion table (cvt) and function (conv)
                        cvt = variables[var]
                        if cvt[1] == "": continue
                        conv = cvt[2]
                        if conv.nin == 1:
                            # Just convert the value found in the dictionary
                            values[var] = conv(summary[cvt[1]])
                        else:
                            # Assume there are 2 input arguments to deal with
                            if ('+' in cvt[1]):
                                x = cvt[1].split('+')
                            else if ('*' in cvt[1]):
                                x = cvt[1].split('*')
                            else if ('-' in cvt[1]):
                                x = cvt[1].split('-')
                            values[var] = conv(summary[x[0]], summary[x[1]])
                    joint_netcdf4.set_data(year, lon, lat, values)
        
            # Now write
            for _ in range(nrows):
                joint_netcdf4.writenext()
        
            # Close all files for this crop
            joint_netcdf4.close()
            
    except SQLAlchemyError as inst:
        msg = "Database error on crop %i." % crop_no
        print msg
        logging.exception(msg)
    except RuntimeError as inst:
        msg = "Error opening netCDF4 dataset on crop %i." % crop_no
        print msg
        logging.exception(msg)
    except PCSEError as inst:
        logging.exception(str(inst))
    finally:
        db_engine = None
  
def get_finished_tasks(engine, crop_no):
    conn = engine.connect()
    sqlStr = """SELECT task_id FROM tasklist
                WHERE crop_no=%s AND status='Finished'"""
    rows = conn.execute(sqlStr % crop_no)
    conn.close()
    return rows
            
def get_crop_info(engine, crop_no):
    crop_label = ""
    mgmt_code = ""
    conn = engine.connect()
    sqlStr = """SELECT m.crop_no, x.label, m.mgmt_code 
             FROM crop m INNER JOIN cropinfo x ON m.crop_no = x.crop_no
             WHERE m.crop_no=%s"""
    rows = conn.execute(sqlStr % crop_no)
    for row in rows:
        crop_label = row["label"]
        mgmt_code = row["mgmt_code"]
        break
    conn.close()
    return crop_label, mgmt_code

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