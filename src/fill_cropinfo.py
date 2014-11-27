from sqlalchemy import engine as sa_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import MetaData, Table
from cropinforeader import CropInfoProvider
import run_settings

def main():
    # Initialise
    recs = []
    db_engine = None;
    
    try:
        # Initialise further
        db_engine = sa_engine.create_engine(run_settings.connstr)
        db_metadata = MetaData(db_engine)
        
        # Now loop over the crops
        for crop_no in range(1, 29):
            # Report which crop is next
            crop_name, mgmt_code = select_crop(db_engine, crop_no)
            msg = "About to get TSUM values for " + crop_name + " (" + mgmt_code + ")"
            print msg
            
            # Now retrieve how to divide over TSUM1 and TSUM2
            cip = CropInfoProvider(crop_name, mgmt_code, run_settings.data_dir)
            cropdata = cip.getCropData();
            fract1, fract2 = split_tsum(cropdata, 1)
            rec = {}
            rec["crop_no"] = crop_no
            rec["fract_tsum1"] = fract1
            rec["fract_tsum2"] = fract2
            recs.append(rec)
        
        cropinfodict = {"crop_no":1, "fract_tsum1":0.4 , "fract_tsum2":0.6}    
        store_to_database(db_engine, recs, db_metadata, cropinfodict)
        
    except Exception as e:
        print str(e)
    finally:
        if db_engine != None: db_engine.dispose()
            
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

    table_results = Table('cropinfo', metadata, autoload=True)
    ic = table_results.insert()
    ic.execute(recs_output)

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


if (__name__ == "__main__"):
    main();