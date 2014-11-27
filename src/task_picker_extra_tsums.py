from __future__ import print_function
import sys;
sys.path.append(r"/home/hoek008/projects/ggcmi/pcse")
from task_runner_extra_tsums import task_runner
from pcse.exceptions import PCSEError
import logging

from pcse.taskmanager import TaskManager
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import engine as sa_engine
import tables

import run_settings

def run_with_taskmanager():
    """Start PCSE/WOFOST with the task manager. All tasks are retrieved
    from a database, all settings are derived from the module run_settings.py.
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
            msg = "Running task: %i" % task_id
            logging.info(msg)
            print(msg)
            task_runner(db_engine, task)

            # Set status of current task to 'Finished'
            taskmanager.set_task_finished(task)

        except SQLAlchemyError as inst:
            msg = "Database error on task_id %i." % task_id
            logging.exception(msg)
            # Break because of error in the database connection
            break

        except PCSEError as inst:
            msg = "Error in PCSE on task_id %i." % task_id
            logging.exception(msg)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task)

        except tables.NoSuchNodeError as e:
            msg = "No weather data found for lat/lon: %s/%s"
            logging.error(msg, task["latitude"], task["longitude"])

        except Exception as inst:
            msg = "General error on task_id %i" % task_id
            logging.exception(msg)
            # Set status of current task to 'Error'
            task["comment"] = str(inst)
            taskmanager.set_task_error(task)

        except KeyboardInterrupt:
            msg = "Terminating on user request!"
            logging.error(msg)
            taskmanager.set_task_error(task)
            sys.exit()

        finally:
            #Get new task
            task = taskmanager.get_task()

if __name__ == "__main__":
    run_with_taskmanager()
