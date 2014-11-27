import sys
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")
from cropinforeader import CropInfoProvider
from pcse.base_classes import WeatherDataProvider
from pcse.fileinput.hdf5reader import Hdf5WeatherDataProvider
from pcse.exceptions import PCSEError
from pcse.engine import Engine as wofostEngine
from numpy import mean, std
from datetime import datetime
from sqlalchemy.schema import MetaData
from sqlalchemy import Table
import time
import logging

import run_settings

def task_runner(sa_engine, task):
    # Get crop_name and mgmt_code
    crop_no = task["crop_no"]
    lat = float(task["latitude"])
    lon = float(task["longitude"])
    year = 1900
    cip = None
    wdp = None
    
    # Get a crop info provider
    try:
        t1 = time.time()
        cropname, watersupply = select_crop(sa_engine, crop_no)
        cip = CropInfoProvider(cropname, watersupply, run_settings.data_dir)
        cropdata = cip.getCropData()
        msg = "Retrieving data for crop %s, %s took %6.1f seconds" 
        print msg % (cropname, watersupply, time.time()-t1)

        # Everything seems to be set for the task now! Lon-lat combination is 
        # checked in the call to the getSeasonDates method. We assume that the
        # landmask was checked. Also we assume that the data on the growing
        # season were checked. These are represented by day-of-year values.
        start_doy, end_doy = cip.getSeasonDates(lon, lat)
        
        # Get the weather data
        wdp = Hdf5WeatherDataProvider(run_settings.hdf5_meteo_file, lat, lon)

        # Loop over the years
        t2 = time.time()
        tsums = []
        for year in get_available_years(wdp):
            # Get timer data for the current year
            timerdata = cip.getTimerData(start_doy, end_doy, year)

            # Check that START_DATE does not fall before the first available 
            # meteodata and END_DATE not beyond the last data with available
            # weather data.
            if timerdata['START_DATE'] < wdp.first_date: continue
            if timerdata['END_DATE'] > wdp.last_date: continue

            sitedata = {}
            soildata = {"SMFCF":0.4}

            # Run simulation
            pheno = wofostEngine(sitedata, timerdata, soildata, cropdata, wdp,
                        config="Wofost71_PhenoOnly.conf")
            pheno.run(days=366)

            sumresults = pheno.get_summary_output()
            if len(sumresults) > 0:
                tsums.append(sumresults[0]["TSUM"])
            else:
                msg = "No summary results for crop/year/lat/lon: %s/%s/%s/%s"
                logging.error(msg, crop_no, year, lat, lon)

        # end year

        # Insert average etc. into database
        if len(tsums) > 0:
            insert_tsum(sa_engine, 1, crop_no, lat, lon, mean(tsums), 
                std(tsums, ddof=1), min(tsums), max(tsums), len(tsums))
            msg = "Simulating for lat-lon (%s, %s) took %6.1f seconds" 
            print msg % (str(lat), str(lon), time.time()-t2)
        else:
            msg = "Failed calculating TSUMs for lat-lon (%s, %s): no TSUMs calculated for 30 years"
            logging.error(msg, lat, lon)

    finally:
        # Clean up
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
        result = range(tmpList[0], tmpList[1])
    return result

def insert_tsum(sa_engine, dataset_id, crop_no, lat, lon, avg, stdev, minval, maxval, numobs):
    metadata = MetaData(sa_engine)
    table_tsum = Table("tsum", metadata, autoload=True)
    cursor = table_tsum.insert()
    rec = {"dataset_id":dataset_id, "crop_no":crop_no, "latitude":lat,
           "longitude":lon, "average":avg, "stdev":stdev, "minimum":minval,
           "maximum":maxval, "numobs":numobs}
    cursor.execute([rec])

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

