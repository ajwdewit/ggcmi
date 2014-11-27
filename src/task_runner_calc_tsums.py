import sys;
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")

# import pcse;
from cropinforeader import CropInfoProvider;
#from pcse.pcse.models import Wofost71_PP;
from pcse.base_classes import WeatherDataProvider
from pcse.fileinput.hdf5reader import Hdf5WeatherDataProvider
from pcse.engine import Engine as wofostEngine;
#from AgmerraNetcdfReader import AgmerraWeatherDataProvider;
from numpy import arange, mean, std;  
from datetime import datetime, date;
from sqlalchemy import engine as sa_engine;
from sqlalchemy.sql.schema import MetaData;
from sqlalchemy import Table;
import time
import logging
# import pdb;


def task_runner(sa_engine, task):
    # Attributes
    cip = None;
    extent = None;
    cropdata = None;
    crop_no = 0;
             
    # Initialise
    datadir = r"/home/hoek008/projects/ggcmi/data/"
    fn = r"./AgMERRA/AgMERRA_1980-01-01_2010-12-31_final.hf5"
    
    # Get crop_name and mgmt_code
    crop_no = task["crop_no"];
    cropname, watersupply = select_crop(sa_engine, crop_no);
    print "About to simulate for " + cropname + " (" + watersupply + ")";
    
    try:
        # Get a crop info provider
        t1 = time.time()
        cip = CropInfoProvider(cropname, watersupply, datadir);
        extent = cip.getExtent();
        cropdata = cip.getCropData();
        msg = ("Retrieving data for crop %s, %s took %6.1f seconds" % (cropname, watersupply, time.time()-t1))
        print msg;
    except Exception as e:
        print str(e); 

    # Everything seems to be set for the task now!
    try:  
        # Make sure we can loop over the grid cells from UL down to LR corner
        nvlp = extent;
        x_range = arange(nvlp.getMinX() + 0.5*nvlp.dx, nvlp.getMaxX() + 0.5*nvlp.dy, nvlp.dx);
        y_range = arange(nvlp.getMinY() + 0.5*nvlp.dy, nvlp.getMaxY() + 0.5*nvlp.dx, nvlp.dy);
        if (nvlp.xcoords_sort != 'ASC'): x_range = reversed(x_range);
        if (nvlp.ycoords_sort != 'DESC'): y_range = reversed(y_range);
        
        # Find out whether maybe another process already calculated TSUMs for this crop
        minlat, maxlon = get_resumption_point(sa_engine, crop_no);
        
        # Loop but make sure not to waste time on pixels that are not interesting
        for lat in y_range:
            if (lat > minlat): continue;
            
            eps = 0.0000001;
            for lon in x_range: 
                if abs(lat - minlat) < eps and (lon - maxlon) < eps: continue;
                
                # Check the landmask
                if not cip.get_landmask(lon, lat): continue;
                
                # Check the data on the growing season; start_day eq.to -99 means that area is
                # usu. only little above crop_specific base temperature and end_day equal to -99
                # means that hot periods need to be avoided - 
                start_day, end_day = cip.getSeasonDates(lon, lat);
                if (start_day == -99) or (end_day == -99):
                    continue;

                # Initialise
                wdp = None
                try: 
                    # Retrieve the relevant weather data
                    wdp = Hdf5WeatherDataProvider(fn, lat, lon, datadir);
                    
                    # Loop over the years
                    t2 = time.time()
                    tsums = [];
                    for year in get_available_years(wdp):
                        try:
                            # Get timer data for the current year
                            timerdata = cip.getTimerData(start_day, end_day, year);
                            sitedata = {};
                            soildata = {"SMFCF":0.4};
                        
                            # Run simulation
                            pheno = wofostEngine(sitedata, timerdata, soildata, cropdata, wdp, config="Wofost71_PhenoOnly.conf")
                            pheno.run(days=366)
                            
                            # Retrieve result
                            results = pheno.get_output()
                            #if (results != None) and isinstance(results, list):
                            #    print results[-1];
                            sumresults = pheno.get_summary_output();
                            print sumresults;
                            if isinstance(sumresults[0], dict) and isinstance(sumresults[0]["TSUM"], float):
                                tsums.append(sumresults[0]["TSUM"]);
                        except Exception, e:
                            print str(e);
                        # end try  
                    # end year 
                    
                    # Insert average etc. into database
                    if len(tsums) > 0:
                        insert_tsum(sa_engine, 1, crop_no, lat, lon, mean(tsums), std(tsums, ddof=1), min(tsums), max(tsums), len(tsums));
                    msg = ("Simulating for lat-lon (%s, %s) took %6.1f seconds" % (str(lat), str(lon), time.time()-t2)) 
                    print msg;
                    
                except Exception, e:
                    print str(e);                                    
                finally:
                    if (wdp != None): 
                        wdp.close();
                # end try        
            # end lon            
        # end lat     
        print "Finished simulating for " + cropname + " (" + watersupply + ")";
        cip.close();
      
    except Exception as e:
        print str(e);
    
def get_available_years(wdp):
    result = []
    if isinstance(wdp, WeatherDataProvider):
        tmpList = [wdp.first_date.year, wdp.last_date.year];
        if wdp.first_date != datetime(wdp.first_date.year, 1, 1).date():
            tmpList[0] = wdp.first_date.year + 1;
        if wdp.last_date != datetime(wdp.last_date.year, 12, 31).date():
            tmpList[1] = wdp.last_date.year - 1;
        result = range(tmpList[0], tmpList[1]);
    return result;
    
def insert_tsum(sa_engine, dataset_id, crop_no, lat, lon, avg, stdev, minval, maxval, numobs):
    try:
        metadata = MetaData(sa_engine)
        table_tsum = Table("tsum", metadata, autoload=True)
        cursor = table_tsum.insert()
        rec = {"dataset_id":dataset_id, "crop_no":crop_no, "latitude":lat, "longitude":lon, "average":avg, "stdev":stdev, "minimum":minval, "maximum":maxval}
        cursor.execute([rec]);
    except Exception as e:
        print str(e);

def select_crop(engine, crop_no):
    crop_name = "";
    mgmt_code = "";
    try:
        conn = engine.connect();
        rows = conn.execute("SELECT crop_no, crop_name, mgmt_code FROM crop WHERE crop_no=" + str(crop_no));
        for row in rows:
            crop_name = row["crop_name"];
            mgmt_code = row["mgmt_code"];
            break;
    except Exception as e:
        print str(e);
    return crop_name, mgmt_code;

def get_resumption_point(engine, crop_no):
    minlat = 90.0;
    maxlon = -180.0;
    try:
        conn = engine.connect();
        rows = conn.execute("select crop_no, min(latitude) as minlat, max(longitude) as maxlon from tsum where crop_no="+ str(crop_no));
        for row in rows: 
            minlat = row["minlat"];
            maxlon = row["maxlon"];
            break;
    except Exception as e:
        print str(e);
    return float(minlat), float(maxlon); 
    
