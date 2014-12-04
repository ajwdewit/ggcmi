from pcse.geo.netcdf4raster import Netcdf4Raster
import os, shutil
import numpy as np

class JointNetcdf4Raster():
    _datasets = {}
    _rasters = {}
    _currow = 0;
    _startyear = 1945
    _end_year = 2050
    
    def __init__(self, path2template, fpath=os.getcwd(), pattern="part1_*_part2.nc4", keys=[]):
        # Locate the template file
        path2template = os.path.normpath(path2template)
        if not os.path.exists(path2template):
            raise IOError("Template %s not found" % path2template)
        
        for key in keys:
            # Compose the name - copy the template file to a file with the right name
            fp = os.path.join(fpath, pattern.replace("*", key))
            shutil.copyfile(path2template, fp)
            os.chmod(fp, 0664)
            self._datasets[key] = Netcdf4Raster(fp)
            
    def open(self, mode, start_year, end_year, ncols=1, nrows=1, xll=0, yll=0, cellsize=1, nodatavalue=-9999.0):
        self._startyear = start_year
        self._end_year = end_year
        for key in self._datasets.keys():
            ds = self._datasets[key]
            if mode[0] == 'a': 
                if ds.open('a', ncols, nrows, xll, yll, cellsize, nodatavalue):
                    nyears = end_year - start_year + 1
                    self._rasters[key] = np.empty((nyears, nrows, ncols), dtype=np.float)
                    self._rasters[key][:, :, :] = nodatavalue
                else:
                    raise IOError("File %s could not be opened" % ds.name)
            else:
                if ds.open('r'):
                    pass
                else:
                    raise IOError("File %s could not be opened" % ds.name)
        return True

    def set_data(self, yrcount, lon, lat, valueDict):
        # Input is a dictionary
        keys = valueDict.keys()
        ds = self._datasets[keys[0]]
        k, i = ds.getColAndRowIndex(lon, lat)
        for key in keys:
            raster = self._rasters[key]
            raster[yrcount, i, k] = valueDict[key] 
        
    def writenext(self):
        for key in self._datasets.keys():
            values = self._rasters[key][:, self._currow,  :] 
            self._datasets[key].writenext(values)
        self._currow += 1
    
    def writeheader(self, key, name, long_name, units):
        self._datasets[key].writeheader(name, long_name, units)
    
    def close(self):
        for key in self._datasets.keys():
            self._datasets[key].close()
        self._datasets = None
        self._rasters = None
