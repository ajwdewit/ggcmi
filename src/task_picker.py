import sys;
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")
from task_runner import task_runner
from pcse.exceptions import PCSEError
import logging

from pcse.taskmanager import TaskManager
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import engine as sa_engine

import run_settings

def run_with_taskmanager(dsn=None):
    """Example for running PCSE/WOFOST with the task manager.

    Runs PyWOFOST for 6 crop types for one location in water-limited mode
    using an ensemble of 50 members. Each member is initialized with different
    values for the initial soil moisture and each member receives different
    values for rainfall as forcing variable. Executing PyWOFOST runs is done
    through the task manager. Output is writted to the database

    Parameters:
    dsn - SQLAlchemy data source name pointing to the database to be used.
    """

    # Open database connection and empty output table
    db_engine = sa_engine.create_engine(run_settings.connstr)

    # Initialise task manager
    taskmanager = TaskManager(db_engine, dbtype="MySQL")
    # Loop until no tasks are left
    task = taskmanager.get_task()
    while task is not None:
        try:
            task_id = task["task_id"]
            print "Running task: %i" % task_id
            task_runner(db_engine, task)

            # Set status of current task to 'Finished'
            taskmanager.set_task_finished(task)

        except SQLAlchemyError as inst:
            msg = "Database error on task_id %i." % task_id
            print msg
            logging.exception(msg)
            # Break because of error in the database connection
            break

        except PCSEError as inst:
            msg = "Error in PCSE on task_id %i." % task_id
            print msg
            logging.exception(msg)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task)

        except Exception as inst:
            msg = "General error on task_id %i" % task_id
            print msg
            logging.exception(msg)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task)

        except KeyboardInterrupt:
            msg = "Terminating on user request!"
            print msg
            logging.error(msg)
            taskmanager.set_task_error(task)
            sys.exit()

        finally:
            #Get new task
            task = taskmanager.get_task()

if __name__ == "__main__":
    run_with_taskmanager()
