import sys
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import engine as sa_engine
import tables

import run_settings
sys.path.append(run_settings.pcse_dir)
from ggcmi_task_runner import task_runner
from pcse.exceptions import PCSEError
from pcse.taskmanager import TaskManager

def run_with_taskmanager():
    """Main script for running PCSE/WOFOST with the task manager.

    All configuration options are retrieved from run_settings.py
    """

    logger = logging.getLogger("GGCMI Task Runner")

    # Open database connection and empty output table
    db_engine = sa_engine.create_engine(run_settings.connstr)

    # Initialise task manager
    taskmanager = TaskManager(db_engine, dbtype="MySQL")

    # Loop until no tasks are left
    task = taskmanager.get_task()
    ntasks = 0
    while task is not None and ntasks < run_settings.max_tasks_per_worker:
        ntasks += 1
        try:
            task_id = task["task_id"]
            print "Running task: %i" % task_id
            task_runner(db_engine, task)

            # Set status of current task to 'Finished'
            taskmanager.set_task_finished(task)

        except SQLAlchemyError as inst:
            msg = "Database error on task_id %i." % task_id
            logger.exception(msg)
            # Break because of error in the database connection
            break

        except PCSEError:
            msg = "Error in PCSE on task_id %i." % task_id
            logger.exception(msg)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task, comment="PCSE Error")

        except tables.NoSuchNodeError:
            msg = "No weather data found for lat/lon: %s/%s"
            logger.error(msg, task["latitude"], task["longitude"])
            taskmanager.set_task_error(task, comment="No weather data")

        except run_settings.NoFAOSoilError as e:
            msg = "No soil data: %s" % e
            logger.error(msg)
            taskmanager.set_task_error(task, comment="No soil data")

        except Exception:
            msg = "General error on task_id %i" % task_id
            logger.exception(msg)
            # Set status of current task to 'Error'
            taskmanager.set_task_error(task, comment="General error, see log.")

        except KeyboardInterrupt:
            msg = "Terminating on user request!"
            logger.error(msg)
            taskmanager.set_task_error(task, comment=msg)
            sys.exit()

        finally:
            #Get new task
            task = taskmanager.get_task()

if __name__ == "__main__":
    run_with_taskmanager()
