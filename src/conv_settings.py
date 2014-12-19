import os

#database connection
username = "hoek008"
password = "alterra"
hostname = "d0116632"
dbname   = "ggcmi"
connstr = 'mysql://%s:%s@%s/%s?charset=utf8' % (username, password, hostname, dbname)

# Folder for pcse code
pcse_dir = r"D:\Userdata\hoek008\GGCMI\PySrc\pcse"

# Top level folder for data
data_dir = r"D:\Userdata\hoek008\GGCMI\phase1\data"
results_folder = os.path.join(data_dir, "..", "results")

# Location where output should be written 
output_folder = os.path.join(data_dir, "output")
output_file_template = "ggcmi_results_task_%010i.pkl"
shelve_folder = os.path.join(data_dir, "shelves")