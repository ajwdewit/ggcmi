"""CropInfoProvider provides the CROP parameter values for running PCSE/WOFOST.

It directly

"""
import os
from datetime import date, timedelta
import numpy as np

from pcse.exceptions import PCSEError
from netCDF4 import Dataset
from pcse.fileinput import CABOFileReader
from pcse.geo.floatingpointraster import FloatingPointRaster
from pcse.geo.netcdf4envelope2d import Netcdf4Envelope2D

import run_settings

class CropInfoProvider():
    regions = run_settings.regions

    # File - path to *.flt file with land mask
    landmask_grid = run_settings.landmask_grid

    # Netcdf dataset with planting and harvest dates
    _ds = None
    _cropdata = None
    _envelope = None

    def __init__(self, crop, watersupply):
        # match the given crop and region with a tuple from the list
        for cropname, _, filename, _ in run_settings.crop_info_sources:
            # for the mean time, don't worry about the region
            if cropname == crop:
                cabo_file = filename
                break
        else:
            msg = "Cannot find CABO file matching crop: %s"
            raise PCSEError(msg)

        # read parameters from the filename stored in _crop_info_sources
        cabo_file_fp = os.path.join(run_settings.cabofile_folder, cabo_file)
        self._cropdata = CABOFileReader(cabo_file_fp)
        if "IOX" not in self._cropdata:
            self._cropdata["IOX"] = 0

        # Prepare to read planting and harvest data from the netcdf file
        crop_calendar_file = (crop + "_" + watersupply +
                              run_settings.growing_season_file_suffix)
        crop_calendar_file_fp = os.path.join(run_settings.growing_season_folder,
                                             crop_calendar_file)
        try:
            self._ds = Dataset(crop_calendar_file_fp, 'r')
            self._envelope = Netcdf4Envelope2D(self._ds)
        except Exception as e:
            msg = "An error occurred while opening file %s: %s" % (
                  crop_calendar_file, e)
            raise PCSEError(msg)

    # @staticmethod
    # def getCropGroup(aCropName):
    #     result = 0
    #     for cropname, _, _, group_no in run_settings.crop_info_sources:
    #         if cropname == aCropName:
    #             result = group_no
    #             break
    #     return result
    #
    # @staticmethod
    # def getCrops():
    #     result = {}
    #     i = 1
    #     for cropname, _, _, _ in run_settings.crop_info_sources:
    #         result[i] = cropname
    #         i = i + 1
    #     return result

    def getCropData(self):
        return self._cropdata

    def getSeasonDates(self, longitude, latitude):
        eps = 0.001

        # Check that the netCDF file is loaded
        if self._ds is None:
            raise PCSEError("file is no more open.")

        # Check that the given latitude and longitude is within extent of the
        # netCDF4 file
        if not self._envelope.isWithinExtent(longitude, latitude):
            msg = "Given lon/lat (%f/%f)coordinates beyond file extent."
            raise PCSEError(msg % (longitude, latitude))
        k, i = self._envelope.getColAndRowIndex(longitude, latitude)

        # Check that the found indices are really linked to the given lat-lon
        msg = " coordinate for this index not as expected: "
        assert abs(self._ds.variables["lat"][i] - latitude) < 0.5*self._envelope.dy + eps, "Y" + msg + str(i)
        assert abs(self._ds.variables["lon"][k] - longitude) < 0.5*self._envelope.dx + eps, "X" + msg + str(k)

        # Take into account that the variables are made out of masked arrays
        start_doy = -99
        end_doy = -99
        try:
            arr_elem = self._ds.variables['planting day'][i, k]
            start_doy = int(arr_elem)
            if start_doy < 0:
                start_doy = -99
            arr_elem = self._ds.variables['harvest day'][i, k]
            end_doy = int(arr_elem)
            if end_doy < 0:
                end_doy = -99
        except np.ma.core.MaskError:
            pass
        return start_doy, end_doy


    def getTimerData(self, start_doy, end_doy, year):

        try:
            # Prepare the timer data
            result = {}
            if end_doy > start_doy:
                # Assume that start and end time are within the same year
                crop_start_date = date(year,1,1) + timedelta(days=start_doy-1)
                crop_end_date = date(year,1,1) + timedelta(days=end_doy-1)
            else:
                # Not within 1 year, assume sowing in previous year
                crop_start_date = date(year-1,1,1) + timedelta(
                    days=start_doy-1)
                crop_end_date = date(year,1,1) + timedelta(days=end_doy-1)


            result['CAMPAIGNYEAR'] = year
            # system start date will be before the crop starts. the number of
            # days depends on the setting "days_before_CROP_START_DATE"
            result['START_DATE'] = crop_start_date - \
                timedelta(days=run_settings.days_before_CROP_START_DATE)
            result['CROP_START_DATE'] = crop_start_date
            result['CROP_START_TYPE'] = 'sowing'
            # we add days to CROP_END_DATE to allow variability in
            # maturity date.
            crop_end_date += timedelta(days=run_settings.days_after_CROP_END_DATE)
            result['CROP_END_DATE'] = crop_end_date
            result['CROP_END_TYPE'] = 'earliest'
            result['END_DATE'] = crop_end_date + timedelta(days=10)
            result['MAX_DURATION'] = 365
            return result
        except Exception as e:
            msg = "An error occurred while preparing the timer data: " + str(e)
            raise PCSEError(msg)

    def close(self):
        if self._ds is not None:
            self._ds.close()
            self._ds = None

    def getExtent(self):
        return self._envelope

    def _getFullPath(self, fname):
        path = os.path.dirname(self._ds.filepath())
        result = os.path.join(path, fname)
        result = os.path.normpath(result)
        return result

    def _get_value_from_grid(self, longitude, latitude, fpath):
        # Open the file. Get right row and column. Elevations are linked to
        # the cell centres
        r = FloatingPointRaster(fpath, "i")
        if not r.open('r'):
            raise Exception("Unable to open input file " + r.name)
        k, i = r.getColAndRowIndex(longitude, latitude)
        if i >= r.nrows:
            msg = "FloatingPointRaster: try to read beyond nrows!"
            raise RuntimeError(msg)

        # Now get hold of the right row, read the wanted value and close
        for _ in range(0, i+1):
            rawline = r.next()
        r.close()
        if k == r.ncols:
            msg = "FloatingPointRaster: try to read beyond ncols!"
            raise RuntimeError(msg)

        line = np.frombuffer(rawline, 'f')
        return line[int(k)]

    def get_landmask(self, longitude, latitude):
        fpath = self._getFullPath(self.landmask_grid)
        value = self._get_value_from_grid(longitude, latitude, fpath)
        return value == 1


