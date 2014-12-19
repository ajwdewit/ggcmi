"""Main script for starting GGCMI runs

"""
import time
import multiprocessing
import logging
from datetime import datetime

import run_settings
from ggcmi_task_picker import run_with_taskmanager

def determine_CPUs():
    """Determines the number of CPUs to use based on multiprocessing
    .cpu_count() and the setting run_settings.number_of_CPU"""
    nCPU = multiprocessing.cpu_count()

    if nCPU == 1:
        return 1

    if run_settings.number_of_CPU is None:
        return nCPU

    user_CPU = int(run_settings.number_of_CPU)
    if user_CPU == 0:
        return 1
    elif user_CPU > 0:
        return min(nCPU, user_CPU)
    else:
        return max(1, user_CPU + nCPU)

def main():
    format = "%(asctime)-15s %(clientip)s %(user)-8s %(message)s"
    logging.basicConfig(filename="ggcmi_main.log", format=format, level=logging.DEBUG)
    logger = logging.getLogger("Top level process handler.")
    # determine number of CPUs to use
    nCPU = determine_CPUs()
    plist = []
    try:
        if nCPU == 1:
            run_with_taskmanager()
        else:
            for i in range(nCPU):
                p = multiprocessing.Process(target=run_with_taskmanager)
                p.start()
                plist.append(p)
            while True:
                msg = "New cycle for checking processes!"
                logger.info(msg)
                new_plist = []
                for p in plist:
                    if not p.is_alive():
                        msg = "Starting new task_picker process"
                        logger.info(msg)
                        p = multiprocessing.Process(target=run_with_taskmanager)
                        p.start()
                    new_plist.append(p)
                plist = new_plist
                time.sleep(10)
    except Exception:
        msg = "Critical error in GGCMI Main process."
        logger.exception(msg)
    except KeyboardInterrupt:
        if plist:
            for proc in plist:
                proc.terminate()
            msg = "Terminated %i processes on user request." % nCPU
        else:
            msg = "Terminated process on user request."
        logger.critical(msg)

if __name__ == "__main__":
    main()
