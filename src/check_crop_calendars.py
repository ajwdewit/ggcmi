"""Script for checking the sanity of the AgGrid crop calendars
"""
import sys, os
import run_settings
import numpy as np
from numpy.ma import MaskedArray
from netCDF4 import Dataset
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

def make_masked_array(dataset, fill_value):
    ar = np.array(dataset)
    invalid = abs(ar - dataset._FillValue) < 0.001
    r = MaskedArray(ar, invalid)
    return r

for crop_name, region, cabo_file, d in run_settings.crop_info_sources:
    for mgmnt in ["rf", "ir"]:
        crop_calendar_fname = "%s_%s%s" % (crop_name, mgmnt, run_settings.growing_season_file_suffix)
        print "Processing: %s" % crop_calendar_fname
        crop_calendar_fp = os.path.join(run_settings.growing_season_folder, crop_calendar_fname)

        # Open the file
        ds = Dataset(crop_calendar_fp, 'r')

        # lon/lat ranges
        lons = np.array(ds.variables["lon"])
        lats = np.array(ds.variables["lat"])

        classcolors = ListedColormap(['Red', 'Silver', 'RoyalBlue', 'Turquoise', 'DarkBlue'], "indexed")

        # planting/harvest doy and growing season length
        t = ds.variables['planting day']
        planting_ma = make_masked_array(t, t._FillValue)
        t = ds.variables['harvest day']
        harvesting_ma = make_masked_array(t, t._FillValue)
        t = ds.variables['growing season length']
        gsl_ma = make_masked_array(t, t._FillValue)

        # Testing for invalid combinations of data/nodata values
        no_crop = np.logical_or(planting_ma == -99, harvesting_ma == -99)
        planting_valid = np.logical_and(planting_ma >= 1, planting_ma <= 366)
        harvesting_valid = np.logical_and(harvesting_ma >= 1, harvesting_ma <= 366)
        valid_crop = np.logical_and(planting_valid, harvesting_valid)

        # checking for harvest doy before planting doy (sowing in year before)
        hdoy_before_pdoy = np.logical_and(harvesting_ma < planting_ma, valid_crop)

        status_map = np.zeros_like(planting_ma, dtype=np.uint8)
        status_map[no_crop] = 1
        status_map[valid_crop] = 2
        status_map[hdoy_before_pdoy] = 4

        fig = plt.figure()
        axes = fig.add_subplot(1,1,1)
        axes.pcolor(lons, lats, status_map, cmap=classcolors, vmin=0, vmax=4)
        axes.set_title("GGCMI Cropping Calendar Check")
        fig.suptitle(crop_calendar_fname)
        plt.text(-175, -60, "no crop", color="Silver")
        plt.text(-175, -67.5, "cropped", color="RoyalBlue")
        plt.text(-175, -75, "cropped with harvest doy < planting doy", color="DarkBlue")
        plt.text(-175, -82.5, "error in calendar file", color="Red")
        png_fname = "%s_%s_integrity.png" % (crop_name, mgmnt)
        png_fname_fp = os.path.join(run_settings.growing_season_folder, png_fname)
        fig.savefig(png_fname_fp, dpi=300)
        plt.close("all")

        # Checking for growing season length < 90 days
        gsl_lt90 = np.logical_and(gsl_ma < 90, valid_crop)

        status_map = np.zeros_like(planting_ma, dtype=np.uint8)
        status_map[no_crop] = 1
        status_map[valid_crop] = 2
        status_map[gsl_lt90] = 3

        fig = plt.figure()
        axes = fig.add_subplot(1,1,1)
        axes.pcolor(lons, lats, status_map, cmap=classcolors, vmin=0, vmax=4)
        axes.set_title("GGCMI Growing Season Length Check")
        fig.suptitle(crop_calendar_fname)
        plt.text(-175, -60, "no crop", color="Silver")
        plt.text(-175, -67.5, "cropped", color="RoyalBlue")
        plt.text(-175, -75, "cropped with season length < 90 days", color="Turquoise")
        plt.text(-175, -82.5, "error in calendar file", color="Red")
        png_fname = "%s_%s_gsl.png" % (crop_name, mgmnt)
        png_fname_fp = os.path.join(run_settings.growing_season_folder, png_fname)
        fig.savefig(png_fname_fp, dpi=300)
        plt.close("all")



