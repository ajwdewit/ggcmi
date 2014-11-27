import sys
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")

from cropinforeader import CropInfoProvider
from pcse.base_classes import WeatherDataProvider
from pcse.fileinput.hdf5reader import Hdf5WeatherDataProvider
from pcse.exceptions import PCSEError
from pcse.engine import Engine as wofostEngine
from numpy import arange, mean, std
from datetime import datetime, date
from sqlalchemy.schema import MetaData
from sqlalchemy import Table
import time
import logging
import pdb
import tables

import run_settings

def task_runner(sa_engine, task):

    # Get crop_name and mgmt_code
    crop_no = task["crop_no"]
    cropname, watersupply = select_crop(sa_engine, crop_no)
    msg = "Starting to simulate for %s (%s)" % (cropname, watersupply)
    print msg

    # Get a crop info provider
    t1 = time.time()
    cip = CropInfoProvider(cropname, watersupply, run_settings.datadir)
    extent = cip.getExtent()
    cropdata = cip.getCropData()
    msg = ("Retrieving data for crop %s, %s took %6.1f seconds" % (cropname, watersupply, time.time()-t1))
    print msg

    # Everything seems to be set for the task now!
    # Make sure we can loop over the grid cells from UL down to LR corner
    nvlp = extent
    x_range = arange(nvlp.getMinX() + 0.5*nvlp.dx, nvlp.getMaxX() + 0.5*nvlp.dy, nvlp.dx)
    y_range = arange(nvlp.getMinY() + 0.5*nvlp.dy, nvlp.getMaxY() + 0.5*nvlp.dx, nvlp.dy)
    if nvlp.xcoords_sort != 'ASC':
        x_range = reversed(x_range)
    if nvlp.ycoords_sort != 'DESC':
        y_range = reversed(y_range)

    # Find out whether maybe another process already calculated TSUMs for this crop
    minlat, maxlon = get_resumption_point(sa_engine, crop_no)

    # Loop but make sure not to waste time on pixels that are not interesting
    for lat in y_range:
        if (lat > minlat):
            continue
        eps = 0.0000001
        for lon in x_range:
            try:
                if abs(lat - minlat) < eps and (lon - maxlon) < eps:
                    continue
                # Check the landmask
                if not cip.get_landmask(lon, lat):
                    continue

                # Check the data on the growing season represented by day-of-year
                # values. If start_doy or end_doy return nodata value (-99), then
                # continue with next grid directly.
                start_doy, end_doy = cip.getSeasonDates(lon, lat)
                if (start_doy == -99) or (end_doy == -99):
                    continue

                wdp = Hdf5WeatherDataProvider(run_settings.hdf5_meteo_file, lat, lon)

                # Loop over the years
                t2 = time.time()
                tsums = []
                for year in get_available_years(wdp):
                    # Get timer data for the current year
                    timerdata = cip.getTimerData(start_doy, end_doy, year)

                    # Check that START_DATE does not fall before the first
                    # available meteodata and END_DATE not beyond the
                    # last data with available weather data.
                    if timerdata['START_DATE'] < wdp.first_date or \
                       timerdata['END_DATE'] > wdp.last_date:
                        continue

                    sitedata = {}
                    soildata = {"SMFCF":0.4}

                    # Run simulation
                    pheno = wofostEngine(sitedata, timerdata, soildata, cropdata,
                                         wdp, config="Wofost71_PhenoOnly.conf")
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
                    insert_tsum(sa_engine, 1, crop_no, lat, lon, mean(tsums), std(tsums, ddof=1),
                                min(tsums), max(tsums), len(tsums))
                    msg = ("Simulating for lat-lon (%s, %s) took %6.1f seconds" % (str(lat), str(lon), time.time()-t2))
                    print msg
                else:
                    msg = "Failed calculating TSUMs for lat-lon (%s, %s): no TSUMs calculated for 30 years"
                    logging.error(msg, lat, lon)

                if wdp is not None:
                    wdp.close()

            except tables.NoSuchNodeError:
                msg = "No weather data found for lat/lon: %s/%s"
                logging.error(msg, lat, lon)
            except PCSEError:
                msg = "Error in PCSE for crop/year/lat/lon: %s/%s/%s/%s"
                logging.exception(msg, crop_no, year, lat, lon)

    print "Finished simulating for " + cropname + " (" + watersupply + ")"
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

def get_resumption_point(engine, crop_no):
    minlat = 90.0
    maxlon = -180.0
    conn = engine.connect()
    sql = "select crop_no, min(latitude) as minlat, max(longitude) as maxlon from tsum where crop_no="+ str(crop_no)
    rows = conn.execute(sql).fetchall()
    for row in rows:
        if row["minlat"] is None:
            break
        minlat = row["minlat"]
        maxlon = row["maxlon"]
    return float(minlat), float(maxlon)
