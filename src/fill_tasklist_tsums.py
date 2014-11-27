import sys;
sys.path.append(r"/home/projects/hoek008/ggcmi/pcse")
import logging;
from sqlalchemy import engine as sa_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, Table
from sqlalchemy.sql.schema import Column
from sqlalchemy import Integer, String, DECIMAL, Float
#from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import inspect as sa_inspect
from cropinforeader import CropInfoProvider
from pcse.exceptions import PCSEError
from numpy import arange

import run_settings
DeclarativeBase = declarative_base()

class Task(DeclarativeBase):
    __tablename__ = "tasklist"
    task_id = Column(Integer, primary_key=True)
    status = Column(String(16), nullable=True)
    hostname = Column(String(50), nullable=True)
    crop_no = Column(Integer, nullable=True)
    longitude = Column(DECIMAL(10,2),  nullable=True)
    latitude = Column(DECIMAL(10,2), nullable=True)
    tsum1 = Column(Float, nullable=True)
    tsum2 = Column(Float, nullable=True)
    process_id = Column(Integer, nullable=True)
    comment = Column(String(70), nullable=True)
    
    def __init__(self, task_id, status, hostname, crop_no, longitude, latitude, tsum1, tsum2, process_id, comment):
        self.task_id = task_id
        self.status = status
        self.hostname = hostname
        self.crop_no = crop_no
        self.longitude = longitude
        self.latitude = latitude
        self.tsum1 = tsum1
        self.tsum2 = tsum2
        self.process_id = process_id
        self.comment = comment;
        
    def as_dict(self):
        # Return the values of the fields in a dictionary
        result = {}
        inspected_obj = sa_inspect(self)
        fields = inspected_obj.attrs._data
        for fldname in fields:
            result[fldname] = inspected_obj.attrs._data[fldname].value
        return result; 
                        
def main():
    # Initialise
    db_engine = None;
    tasks = []
    try:
        db_engine = sa_engine.create_engine(run_settings.connstr)
        db_metadata = MetaData(db_engine)
        task_id = 15051
        for crop_no in range(19, 29):
            # Report which crop is next
            crop_name, mgmt_code = select_crop(db_engine, crop_no)
            msg = "About to get TSUM values for " + crop_name + " (" + mgmt_code + ")"
            logging.info(msg)
            print msg
            
            # Now retrieve how to divide over TSUM1 and TSUM2
            cip = CropInfoProvider(crop_name, mgmt_code, run_settings.data_dir)
            cropdata = cip.getCropData();
            nvlp = cip.getExtent()

            # Get the relevant spatial range and loop through
            y_range = get_range(db_engine, crop_no, nvlp.dy)
            x_range = arange(nvlp.getMinX() + 0.5*nvlp.dx, nvlp.getMaxX() + 0.5*nvlp.dy, nvlp.dx); 
            taskdict = {"task_id":1, "status":"Pending" , "hostname":"None", "crop_no":1, "longitude":0.0, "latitude": 0.0, "tsum1":0.0, "tsum2":0.0, "process_id":1, "comment":""}
            for lat in y_range:
                # Retrieve the lat-lon combinations for which there are records in the table TSUM
                lons_with_tsums = get_places_with_tsums(db_engine, crop_no, lat)
                for lon in x_range:
                    # Pixel should be on land, without tsum so far and there should be a crop calendar for it
                    if not cip.get_landmask(lon, lat):
                        continue;
                    if lon in lons_with_tsums:
                        continue;
                    start_doy, end_doy = cip.getSeasonDates(lon, lat);
                    if (start_doy == -99) or (end_doy == -99):
                        continue;

                    # tsum = get_tsum_from_db(db_engine, crop_no, lon, lat)
                    tsum1, tsum2 = (0, 0) # split_tsum(cropdata, tsum)
                    
                    # Create a task and append it
                    task = Task(task_id, "Pending", "None", crop_no, lon, lat, tsum1, tsum2, 0, "")
                    tasks.append(task.as_dict())
                    task_id = task_id + 1
                    
                    # Once in a while, write them to the database
                    if (task_id % 1000 == 0):
                        store_to_database(db_engine, tasks, db_metadata, taskdict)
                        del tasks[:]
                        print "Thousand records saved to database. Current latitude is %s" % lat
                # end lon
            # end lat
            store_to_database(db_engine, tasks, db_metadata, taskdict)
            print "Another %s records saved to database." % len(tasks)
            del tasks[:]        
                    
    except SQLAlchemyError, inst:
        print ("Database error: %s" % inst)

    except PCSEError, inst:
        print "Error in PCSE: %s" % inst

    except BaseException, inst:
        print ("General error: %s" % inst)
    finally:
        if db_engine != None: db_engine.dispose()
    
def split_tsum(cropdata, tsum):
    try:   
        # Now find out how to divide over TSUM1 and TSUM2
        tsum1 = float(cropdata["TSUM1"])
        tsum2 = float(cropdata["TSUM2"])
        denom = tsum1 + tsum2
        result = (tsum1 / denom) * tsum, (tsum2 / denom) * tsum
        return result
    except Exception as e:
        raise e

def get_tsum_from_db(db_engine, crop_no, longitude, latitude):
    result = -1;
    conn = None
    try:
        # Get TSUM for nearest place with latitude < 0.5 * cellsize 
        conn = db_engine.connect();
        sqlStr = """select crop_no, longitude, latitude, average from tsum 
                 where crop_no=%s and abs(latitude-(%s))<0.25
                 order by abs(longitude-(%s)) limit 1"""
        sqlStr = sqlStr % (crop_no, latitude, longitude)
        rows = conn.execute(sqlStr)
        for row in rows:
            result = row["average"];
            break
    except Exception as e:
        raise e
    finally:
        if conn != None: conn.close()
    return result

def select_crop(db_engine, crop_no):
    crop_name = "";
    mgmt_code = "";
    conn = None
    try:
        conn = db_engine.connect();
        rows = conn.execute("SELECT crop_no, crop_name, mgmt_code FROM crop WHERE crop_no=" + str(crop_no));
        for row in rows:
            crop_name = row["crop_name"];
            mgmt_code = row["mgmt_code"];
            break;
    except Exception as e:
        raise e;
    finally:
        if conn != None: conn.close()
    return crop_name, mgmt_code;

def get_places_with_tsums(db_engine, crop_no, lat):
    # Retrieve all the longitudes for which there are already TSUMS in the table
    result = []
    try:
        sqlStr = """SELECT longitude FROM tsum WHERE latitude = %s AND crop_no = %s"""
        sqlStr = sqlStr % (lat, crop_no)
        conn = db_engine.connect();
        rows = conn.execute(sqlStr)
        for row in rows:
            result.append(row[0]) 
    except Exception as e:
        raise e;
    finally:
        if conn != None: conn.close()
    return result    

def get_range(db_engine, crop_no, cellsize):
    result = (0, 0)
    conn = None
    try:
        sqlStr = """SELECT crop_no, min(latitude), max(latitude) FROM tsum WHERE crop_no=%s"""
        conn = db_engine.connect();
        rows = conn.execute(sqlStr % crop_no);
        for row in rows:
            result = arange(float(row[1]), float(row[2]) + cellsize, cellsize)
            break
    except Exception as e:
        raise e;
    finally:
        if conn != None: conn.close()
    return result; 

def store_to_database(self, recs_output, metadata=None, runid=None):
    """Stores saved variables of the model run in a database table.

    :param metadata: An SQLAlchemy metadata object providing access to the
                     database where the table 'sim_results_timeseries' can be
                     found.
    :param runid:    A dictionary providing the values for the database
                     columns that 'describe' the WOFOST run. For CGMS this
                     would be the CROP_NO, GRID_NO, YEAR thus the runid
                     would be for example be:
                     `runid={'GRID_NO':1000, 'CROP_NO':1, 'YEAR':2000}`

    Note that the records are written directly to this table. No checks on
    existing records are being carried out.
    """

    if not isinstance(runid, dict):
        msg = ("Keyword 'runid' should provide the database columns "+
               "describing the WOFOST run.")
        raise PCSEError(msg)

    if not isinstance(metadata, MetaData):
        msg = ("Keyword metadata should provide an SQLAlchemy " +
               "MetaData object.")
        raise PCSEError(msg)

    # Insert the records

    table_results = Table('tasklist', metadata, autoload=True)
    ic = table_results.insert()
    ic.execute(recs_output)
        
if (__name__ == "__main__"):
    main();