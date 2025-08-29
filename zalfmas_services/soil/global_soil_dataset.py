#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

from netCDF4 import Dataset
import numpy as np

class GlobalSoilDataSet:
    """Global Soil Dataset for Earth System Modeling"""
    def __init__(self, path_to_soil_dir, resolution):
        # open netcdfs
        path_to_soil_netcdfs = path_to_soil_dir + "/" + resolution + "/"
        if resolution == "5min":
            self.soil_data = {
                "sand": {"var": "SAND", "file": "SAND5min.nc", "conv_factor": 0.01},  # % -> fraction
                "clay": {"var": "CLAY", "file": "CLAY5min.nc", "conv_factor": 0.01},  # % -> fraction
                "corg": {"var": "OC", "file": "OC5min.nc", "conv_factor": 0.01},  # scale factor
                "bd": {"var": "BD", "file": "BD5min.nc", "conv_factor": 0.01 * 1000.0},  # scale factor * 1 g/cm3 = 1000 kg/m3
            }
        else:
            self.soil_data = None  # ["Sand5min.nc", "Clay5min.nc", "OC5min.nc", "BD5min.nc"]
        self.soil_datasets = {}
        self.soil_vars = {}
        for elem, data in self.soil_data.items():
            ds = Dataset(path_to_soil_netcdfs + data["file"], "r", format="NETCDF4")
            self.soil_datasets[elem] = ds
            self.soil_vars[elem] = ds.variables[data["var"]]

    def create_soil_profile(self, row, col):
        # skip first 4.5cm layer and just use 7 layers
        layers = []

        layer_depth = 8
        # find the fill value for the soil data
        for elem2 in self.soil_data.keys():
            for i in range(8):
                if np.ma.is_masked(self.soil_vars[elem2][i, row, col]):
                    if i < layer_depth:
                        layer_depth = i
                    break
                    # return None
        layer_depth -= 1

        if layer_depth < 4:
            return None

        for i, real_depth_cm, monica_depth_m in [(0, 4.5, 0), (1, 9.1, 0.1), (2, 16.6, 0.1), (3, 28.9, 0.1),
                                                 (4, 49.3, 0.2), (5, 82.9, 0.3), (6, 138.3, 0.6), (7, 229.6, 0.7)][1:]:
            if i <= layer_depth:
                layers.append({
                    "Thickness": [monica_depth_m, "m"],
                    "SoilOrganicCarbon": [self.soil_vars["corg"][i, row, col] * self.soil_data["corg"]["conv_factor"], "%"],
                    "SoilBulkDensity": [self.soil_vars["bd"][i, row, col] * self.soil_data["bd"]["conv_factor"], "kg m-3"],
                    "Sand": [self.soil_vars["sand"][i, row, col] * self.soil_data["sand"]["conv_factor"], "fraction"],
                    "Clay": [self.soil_vars["clay"][i, row, col] * self.soil_data["clay"]["conv_factor"], "fraction"]
                })
        return layers