import sys;
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")
from task_runner_calc_tsums import task_runner;
from pcse.exceptions import PCSEError;

def main():
    # Some constants:
    username = "hoek008"
    password = "alterra"
    hostname = "d0116632"
    dbname   = "ggcmi"
    connstr = 'mysql://%s:%s@%s/%s?charset=utf8&use_unicode=0' % (username, password, hostname, dbname);
    run_with_taskmanager(connstr); 

def run_with_taskmanager(dsn=None):
    """Example for running PCSE/WOFOST with the task manager.
    
    Runs PyWOFOST for a number of crop types but only in order to 
	simulate phenology. Executing PyWOFOST runs is done through
     the task manager. Depending on the state of the table TSUM
	 in the database the simulations are done for the whole world
	 or at least for a considerable part of it. Output is written 
	 to the database.
    
    Parameters:
    dsn - SQLAlchemy data source name pointing to the database to be used.
    """

    #from pcse.pywofost_ensemble import PyWofostEnsemble
    from pcse.db.pcse import db_input as dbi
    from pcse.taskmanager import TaskManager
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy import engine as sa_engine
    from sqlalchemy.sql.schema import MetaData
    from sqlalchemy import Table
    import socket
    # from sqlalchemy.exceptions import SQLAlchemyError
    
    # Open database connection and empty output table
    db_engine = sa_engine.create_engine(dsn)
    connection = db_engine.connect()
    metadata = MetaData(db_engine)
    table_tasklist = Table("tasklist", metadata, autoload=True)
    
    # Initialise task manager
    taskmanager = TaskManager(metadata, connection, dbtype="MySQL",
                              hostname=socket.gethostname())
    # Loop until no tasks are left
    task = taskmanager.get_task()
    while task is not None:
        try:
            print "Running task: %i" % (task["task_id"])
            task_runner(db_engine, task);

            # Set status of current task to 'Finished'
            check_connection(connection);
            taskmanager.set_task_finished(task)

        except SQLAlchemyError, inst:
            print ("Database error: %s" % inst)
            # Break because of error in the database connection
            break

        except PCSEError, inst:
            print "Error in PCSE: %s" % inst
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task)

        except BaseException, inst:
            print ("General error: %s" % inst)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task)

        finally:
            #Get new task
            task = taskmanager.get_task()

def check_connection(conn):
    try:
        conn.execute("SELECT count(*) FROM tasklist");
    except Exception as e:
        print str(e);
        print "It seems that the connection was lost, but it's hopefully being restored now ...";

if (__name__ == "__main__"):
    main();