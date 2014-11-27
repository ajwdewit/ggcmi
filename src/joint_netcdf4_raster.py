from pcse.geo.netcdf4raster import Netcdf4Raster
import os, shutil
import numpy as np

class JointNetcdf4Raster():
    _files = {}
    _rasters = {}
    _currow = 0;
    _startyear = 1945
    _end_year = 2050
    
    def __init__(self, path2template, fpath=os.getcwd(), pattern="part1_*_part2.nc4", keys):
        # Locate the template file
        if not os.path.exists(path2template):
            raise IOError("Template %s not found" % path2template)
        
        for key in keys:
            # Compose the name - copy the template file to a file with the right name
            fp = os.path.join(fpath, pattern.replace("*", key))
            shutil.copyfile(path2template, fp)
            self._files[key] = Netcdf4Raster(fp)
            
    def open(self, mode, start_year, end_year, ncols=1, nrows=1, xll=0, yll=0, cellsize=1, nodatavalue=-9999.0):
        self._startyear = start_year
        self._end_year = end_year
        for key in self._files.keys:
            f = self._files[key]
            if mode[0] == 'w': 
                if f.open('w', ncols, nrows, xll, yll, cellsize, nodatavalue):
                    # Some of the metadata has to be changed
                    f.writeheader()
                    nyears = end_year - start_year + 1
                    self._rasters[key] = np.empty((nyears, nrows, ncols), dtype=np.float)
                    self._rasters[key][:, :, :] = nodatavalue
                else:
                    raise IOError("File %s could not be opened" % f.name)
            else:
                if f.open('r'):
                    pass
                else:
                    raise IOError("File %s could not be opened" % f.name)
        return True

    def set_data(self, year, lon, lat, valueDict):
        # Input is a dictionary
        keys = valueDict.keys
        k, i = self._rasters[keys[0]].getColAndRowIndex(lon, lat)
        y = range(self._startyear, self._end_year + 1).index(year)
        for key in keys:
            self._rasters[key][y, i, k] = valueDict[key] 
        
    def writenext(self):
        for key in self._files.keys:
            values = self._rasters[key][:, self._currow,  :] 
            self._files[key].writenext(values)
    
    def writeheader(self, key, name, long_name, units):
        self._files[key].writeheader(name, long_name, units)
    
    def close(self):
        for key in self._files.keys:
            self._files[key].close()
        self._files = None
        self._rasters = None
