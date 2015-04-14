import os

#database connection
username = "test"
password = "xG5^&Hn6VT4k"
hostname = "alterra-ei-ggcmi.cihy1ytynivm.us-west-2.rds.amazonaws.com"
dbname   = "ggcmi_wfdei"
connstr = 'mysql://%s:%s@%s/%s?charset=utf8' % (username, password, hostname, dbname)

# Folder for pcse code
pcse_dir = r"/mnt/ggcmi_output/ggcmi_src/ggcmi/pcse"

# Top level folder for data
data_dir = r"/mnt/ggcmi_output/ggcmi_src/data"
results_folder = os.path.join(data_dir, "../..", "results")
results_folder = os.path.normpath(results_folder)

# Location where output should be written 
output_folder = os.path.join(data_dir, "../..", "output")
output_folder = os.path.normpath(output_folder)
output_file_template = "ggcmi_results_task_%010i.pkl"
shelve_folder = os.path.join(data_dir, "../..", "shelves")
shelve_folder = os.path.normpath(shelve_folder)

# Number of CPU's to use for simulations
# Several has options are possible:
# * None: use the amount of CPUs available as reported by
#   multiprocessing.cpu_count()
# * a positive integer number indicates the number of CPUs to use but
#   with a maximum of multiprocessing.cpu_count()
# * a negative integer number will be subtracted from multiprocessing
# .cpu_count() with a minimum of 1 CPU
number_of_CPU = -2