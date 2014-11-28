import sys
import os
import time
import logging
import cPickle
from datetime import datetime

import run_settings
sys.path.append(run_settings.pcse_dir)
from pcse.base_classes import WeatherDataProvider
from pcse.fileinput.hdf5reader import Hdf5WeatherDataProvider
from pcse.engine import Engine as wofostEngine
from pcse.exceptions import PCSEError
from cropinforeader import CropInfoProvider

def task_runner(sa_engine, task):
    # Get crop_name and mgmt_code
    crop_no = task["crop_no"]
    lat = float(task["latitude"])
    lon = float(task["longitude"])
    year = 1900
    cip = None
    wdp = None
    logger = logging.getLogger("GGCMI Task Runner")
    logger.info("Starting task runner for task %i" % task["task_id"])
    
    # Get a crop info provider
    try:
        t1 = time.time()
        cropname, watersupply = select_crop(sa_engine, crop_no)
        cip = CropInfoProvider(cropname, watersupply, )
        cropdata = cip.getCropData()
        if (not task.has_key("tsum1")) or (not task.has_key("tsum2")):
            msg = "Location specific values for crop parameter(s) missing: TSUM1 and/or TSUM2"
            raise PCSEError(msg)
        else:
            cropdata["TSUM1"] = float(task["tsum1"])
            cropdata["TSUM2"] = float(task["tsum2"])
        start_doy, end_doy = cip.getSeasonDates(lon, lat)
        msg = "Retrieving data for crop %s, %s took %6.1f seconds" 
        logger.info(msg % (cropname, watersupply, time.time()-t1))

        # Everything seems to be set for the task now! Lon-lat combination is 
        # checked in the call to the getSeasonDates method. We assume that the
        # landmask was checked. Also we assume that the data on the growing
        # season were checked. These are represented by day-of-year values.

        # Get the weather data
        t2 = time.time()
        wdp = Hdf5WeatherDataProvider(run_settings.hdf5_meteo_file, lat, lon)
        available_years = get_available_years(wdp)
        msg = "Retrieving weather data for lat-lon %s, %s took %6.1f seconds" 
        logger.debug(msg % (str(lat), str(lon), time.time()-t2))

        # Loop over the years
        t3 = time.time()
        allresults = []
        msg = None
        for year in available_years:
            # Get timer data for the current year
            timerdata = cip.getTimerData(start_doy, end_doy, year)

            # Check that START_DATE does not fall before the first available 
            # meteodata and END_DATE not beyond the last data with available
            # weather data.
            if timerdata['START_DATE'] < wdp.first_date:
                continue
            if timerdata['END_DATE'] > wdp.last_date:
                continue
            # Get soil and site data

            soildata = run_settings.get_soil_data(lon, lat)
            sitedata = run_settings.get_site_data(soildata)

            # Run simulation
            if watersupply == 'ir':
                configFile = 'GGCMI_PP.conf'
            else:
                configFile = 'GGCMI_WLP.conf'

            if msg is None:
                msg = "Starting simulation for %s-%s (%5.2f, %5.2f), planting " \
                      "at: %s, final harvest at: %s"
                msg = msg % (cropname, watersupply, lon, lat, timerdata['CROP_START_DATE'],
                             timerdata['CROP_END_DATE'])
                logger.info(msg)

            wofost = wofostEngine(sitedata, timerdata, soildata, cropdata, wdp,
                                  config=configFile)
            wofost.run_till_terminate()
            results = wofost.get_output()
            sumresults = wofost.get_summary_output()
            if (len(results) > 0) and (len(sumresults) > 0):
                allresults.append({"year":year, "summary":sumresults,
                                   "results":results})
            else:
                msg = "Insufficient results for crop/year/lat/lon: %s/%s/%s/%s"
                logger.error(msg, crop_no, year, lat, lon)
        # end year    
            
        if len(allresults) > 0:
            # pickle all results
            task_id = task["task_id"]
            obj = {"task_id":task_id, "crop_no":crop_no, "longitude":lon, "latitude":lat}
            obj["allresults"] = allresults

            # Write results to pickle file. First to .tmp then rename to .pkl
            # to avoid read/write collisions with ggcmi_processsor
            pickle_fname = run_settings.output_file_template % task_id
            if os.path.splitext(pickle_fname)[1] == ".pkl":
                pickle_fname += ".tmp"
            else:
                pickle_fname += ".pkl.tmp"

            pickle_fname_fp = os.path.join(run_settings.output_folder,
                                           pickle_fname)
            with open(pickle_fname_fp, 'wb') as f:
                cPickle.dump(obj, f, cPickle.HIGHEST_PROTOCOL)
            final_fname_fp = os.path.splitext(pickle_fname_fp)[0]
            os.rename(pickle_fname_fp, final_fname_fp)

            msg = "Simulating for lat-lon (%s, %s) took %6.1f seconds"
            logger.info( msg % (str(lat), str(lon), time.time()-t3))
        else:
            msg = "Failed simulating for lon,lat,crop, watersupply (%s, %s, " \
                  "%s, %s): no output available."
            msg = msg % (lon, lat, cropname, watersupply)
            raise PCSEError(msg)

    finally:
        if wdp is not None:
            wdp.close()
        if cip is not None:
            cip.close()

def get_available_years(wdp):
    result = []
    if isinstance(wdp, WeatherDataProvider):
        tmpList = [wdp.first_date.year, wdp.last_date.year]
        if wdp.first_date != datetime(wdp.first_date.year, 1, 1).date():
            tmpList[0] = wdp.first_date.year + 1
        if wdp.last_date != datetime(wdp.last_date.year, 12, 31).date():
            tmpList[1] = wdp.last_date.year - 1
        result = range(tmpList[0], tmpList[1] + 1)
    return result

def select_crop(engine, crop_no):
    crop_name = ""
    mgmt_code = ""
    conn = engine.connect()
    rows = conn.execute("SELECT crop_no, crop_name, mgmt_code FROM crop WHERE crop_no=" + str(crop_no))
    for row in rows:
        crop_name = row["crop_name"]
        mgmt_code = row["mgmt_code"]
        break
    return crop_name, mgmt_code

