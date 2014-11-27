# -*- coding: utf-8 -*-
# Copyright (c) 2004-2014 Alterra, Wageningen-UR
# Steven Hoek (steven.hoek@wur.nl), August 2014
import os
import time

import tables;
from datetime import datetime;
from ..exceptions import PCSEError
from ..geo.gridenvelope2d import GridEnvelope2D;
from ..base_classes import WeatherDataContainer, WeatherDataProvider;


class Hdf5WeatherDataProvider(WeatherDataProvider, GridEnvelope2D):
    """WeatherDataProvider for using HDF5 files with PCSE

    :param h5fname: filename of HDF5 file containing weather data
    :param latitude: latitude to request weather data for
    :param longitude: longitude to request weather data for

    Weather data can efficiently be delivered in the form of files in the HDF5
    format. In general such data pertain to gridded weather, meaning that they are
    interpolated for a grid with regular intervals in space and time.

    We assume that the grid conforms to one of the WGS standards with longitude
    representing the west to east direction and latitudes the north to south
    direction.

    This class can be uses with different datasets, e.g. having different start year,
    end year and sometimes different variables for representing the weather.

    """
    supports_ensembles = False;

    # Some attributes
    hdf5_file = None;
    nrows = 1;
    ncols = 1;
    xll = -180.0;
    yll = -90.0;
    cellsize = 0.5;
    grp_templ = "";
    tbl_templ = "";
    variables = [];

    # This WeatherDataProvider reads from HDF5 file. Pickling of data is not necessary!
    def __init__(self, fname, latitude, longitude, fpath=None):
        WeatherDataProvider.__init__(self);

        #pdb.set_trace()
        # Check input and process it
        if latitude < -90 or latitude > 90:
            msg = "Latitude should be between -90 and 90 degrees."
            raise ValueError(msg)
        if longitude < -180 or longitude > 180:
            msg = "Longitude should be between -180 and 180 degrees."
            raise ValueError(msg)
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        msg = "Retrieving weather data from file '" + fname + "' for lat/lon: (%f, %f)."
        self.logger.debug(msg % (self.latitude, self.longitude))

        # Construct search path and open the file
        self.hdf5_file = self._get_hdf5_file(fname, fpath);

        # Read attributes, assign them to the envelope and calculate the nearest point
        self.read_attributes();
        GridEnvelope2D.__init__(self, self.ncols, self.nrows, self.xll, self.yll, self.cellsize, self.cellsize);
        self.longitude, self.latitude = self.getNearestCenterPoint(longitude, latitude);

        # Retrieve the records for this location and store them
        self._get_and_process_hdf5();

        self.close()

    def _get_hdf5_file(self, fname, search_path):
        """Find the HDF5 file on given path with given name
        """
        if search_path is None:
            # assume HDF5 file in current folder
            p = os.path.join(os.getcwd(), fname);
        elif os.path.isabs(search_path):
            # absolute path specified
            p = os.path.join(search_path, fname);
        else:
            # assume path relative to current folder
            p = os.path.join(os.getcwd(), search_path, fname);
        hdf5_filename = os.path.normpath(p);

        if not os.path.exists(hdf5_filename):
            msg = "No HDF5 file found when searching at %s"
            raise PCSEError(msg % search_path);

        # Now try to get hold of the hdf5 file
        try:
            # Open and retrieve some attributes that we've added
            result = tables.open_file(hdf5_filename, 'r')
            return result
        except Exception as e:
            msg = "Failed to open the HDF5 file " + fname;
            raise PCSEError(msg + str(e));

    def read_attributes(self):
        if self.hdf5_file:

            # Initialise
            f = self.hdf5_file;

            # Get the attributes wrt. the georeference
            self.ncols = f.root._v_attrs.ncols;
            self.nrows = f.root._v_attrs.nrows;
            self.xll = f.root._v_attrs.xllcorner;
            self.yll = f.root._v_attrs.yllcorner;
            self.cellsize = f.root._v_attrs.cellsize;
            self.NODATA_value = f.root._v_attrs.NODATA_value;

            # Get the variable names
            tvar = f.get_node(f.root, "variables", classname='Array').read();
            # Force variable names to be upper case
            self.variables = [var.upper() for var in tvar]

            # Get the group and table templates
            group_prefix = f.root._v_attrs.group_prefix;
            table_prefix = f.root._v_attrs.table_prefix;
            index_format = f.root._v_attrs.index_format;
            self.grp_templ = group_prefix + "_" + index_format;
            self.tbl_templ = table_prefix + "_" + index_format
        else:
            raise PCSEError("HDF5 file not open!");

    def _get_and_process_hdf5(self):
        if self.hdf5_file:
            # Get the rows
            k, i = self.getColAndRowIndex(self.longitude, self.latitude);
            f = self.hdf5_file;
            t1 = time.time()
            grp = f.get_node(f.root, self.grp_templ % i);
            tbl = f.get_node(grp, self.tbl_templ % k);
            rows = tbl.read();

            # Assign some extra attributes - what if value == nodata_value???
            self.elevation = tbl._v_attrs.elevation;
            self.description = "Meteo data from HDF5 file " + f.filename;
            t2 = time.time()

            # Now store them
            self._make_WeatherDataContainers(rows);
            t3 = time.time()
            self.logger.debug("Reading HDF5 took %7.4f seconds" % (t2-t1))
            self.logger.debug("Processing rows took %7.4f seconds" % (t3-t2))

    def _make_WeatherDataContainers(self, recs):
        # Prepare to loop over all the rows derived from the table
        for row in recs:
            row = tuple(row)
            t = {"LAT": self.latitude, "LON": self.longitude, "ELEV": self.elevation}
            t["DAY"] = datetime.fromordinal(row[0]).date();
            t.update(zip(self.variables, row[1:]))

            # Build weather data container from dict 't'
            wdc = WeatherDataContainer(**t)

            # add wdc to dictionary for this date
            self._store_WeatherDataContainer(wdc, wdc.DAY)


    def _find_cache_file(self, longitude, latitude):
        """Try to find a cache file for given latitude/longitude.
        Returns None if the cache file does not exist, else it returns the full path
        to the cache file.
        """
        raise NotImplementedError("Method not applicable for " + str(self.__class__.__name__));

    def _get_cache_filename(self, longitude, latitude):
        """Constructs the filename used for cache files given latitude and longitude
        The latitude and longitude is coded into the filename - no truncating.
        """
        raise NotImplementedError("Method not applicable for " + str(self.__class__.__name__));

    def _write_cache_file(self):
        """Write the data loaded from the Netcdf files to a binary file using cPickle
        """
        raise NotImplementedError("Method not applicable for " + str(self.__class__.__name__));

    def _load_cache_file(self):
        """Load the weather data from a binary file using cPickle.

        Also checks if any of the Netcdf4 files have modification/creation date more recent then the cache_file.
        In that case reload the weather data from the original Netcdf files.

        Returns True if loading succeeded, False otherwise
        """
        raise NotImplementedError("Method not applicable for " + str(self.__class__.__name__));

    def close(self):
        # Finally close the file
        if self.hdf5_file:
            self.hdf5_file.close();
            self.hdf5_file = None;


