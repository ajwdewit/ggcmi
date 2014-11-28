# Batch script for starting GGCMI batch runs

# First killall running ipython processes
killall -q ipython

# Start the logserver
screen -S logserver -d -m ipython simple_logserver.py 

# start the GGCMI output processor
screen -S "output processor" -d -m ipython ggcmi_process_results.py

# start the GGCMI main process
screen -S "Main" -d -m ipython ggcmi_main.py

