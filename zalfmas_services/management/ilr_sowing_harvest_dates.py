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

import csv
from datetime import date, timedelta
import numpy as np
from pyproj import Transformer
from scipy.interpolate import NearestNDInterpolator

def read_data_and_create_seed_harvest_geo_grid_interpolator(
    path_to_csv_file, wgs84_crs, target_crs, ilr_seed_harvest_data
):
    """read seed/harvest dates and point climate stations"""

    crop_id_to_is_wintercrop = {
        "WW": True,
        "SW": False,
        "WR": True,
        "WRa": True,
        "WB": True,
        "SM": False,
        "GM": False,
        "SBee": False,
        "SU": False,
        "SB": False,
        "SWR": True,
        "CLALF": False,
        "PO": False,
    }

    with open(path_to_csv_file) as _:
        reader = csv.reader(_)

        # print "reading:", path_to_csv_file

        # skip header line
        next(reader)

        points = []  # climate station position (lat, long transformed to a geoTargetGrid, e.g gk5)
        values = []  # climate station ids

        transformer = Transformer.from_crs(wgs84_crs, target_crs, always_xy=True)

        prev_cs = None
        prev_lat_lon = [None, None]
        # data_at_cs = defaultdict()
        for row in reader:
            # first column, climate station
            cs = int(row[0])

            # if new climate station, store the data of the old climate station
            if prev_cs is not None and cs != prev_cs:
                llat, llon = prev_lat_lon
                # r_geoTargetGrid, h_geoTargetGrid = transform(worldGeodeticSys84, geoTargetGrid, llon, llat)
                r_geoTargetGrid, h_geoTargetGrid = transformer.transform(llon, llat)

                points.append([r_geoTargetGrid, h_geoTargetGrid])
                values.append(prev_cs)

            crop_id = row[3]
            is_wintercrop = crop_id_to_is_wintercrop[crop_id]
            ilr_seed_harvest_data[crop_id]["is-winter-crop"] = is_wintercrop

            base_date = date(2001, 1, 1)

            sdoy = int(float(row[4]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["sowing-doy"] = sdoy
            sd = base_date + timedelta(days=sdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["sowing-date"] = {
                "year": 0,
                "month": sd.month,
                "day": sd.day,
            }  # "0000-{:02d}-{:02d}".format(sd.month, sd.day)

            esdoy = int(float(row[8]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["earliest-sowing-doy"] = esdoy
            esd = base_date + timedelta(days=esdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["earliest-sowing-date"] = {
                "year": 0,
                "month": esd.month,
                "day": esd.day,
            }  # "0000-{:02d}-{:02d}".format(esd.month, esd.day)

            lsdoy = int(float(row[9]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["latest-sowing-doy"] = lsdoy
            lsd = base_date + timedelta(days=lsdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["latest-sowing-date"] = {
                "year": 0,
                "month": lsd.month,
                "day": lsd.day,
            }  # "0000-{:02d}-{:02d}".format(lsd.month, lsd.day)

            digit = 1 if is_wintercrop else 0
            if crop_id == "CLALF":
                digit = 2

            hdoy = int(float(row[6]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["harvest-doy"] = hdoy
            hd = base_date + timedelta(days=hdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["harvest-date"] = {
                "year": 0,
                "month": hd.month,
                "day": hd.day,
            }  # "000{}-{:02d}-{:02d}".format(digit, hd.month, hd.day)

            ehdoy = int(float(row[10]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["earliest-harvest-doy"] = ehdoy
            ehd = base_date + timedelta(days=ehdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["earliest-harvest-date"] = {
                "year": digit,
                "month": ehd.month,
                "day": ehd.day,
            }  # "000{}-{:02d}-{:02d}".format(digit, ehd.month, ehd.day)

            lhdoy = int(float(row[11]))
            ilr_seed_harvest_data[crop_id]["data"][cs]["latest-harvest-doy"] = lhdoy
            lhd = base_date + timedelta(days=lhdoy - 1)
            ilr_seed_harvest_data[crop_id]["data"][cs]["latest-harvest-date"] = {
                "year": digit,
                "month": lhd.month,
                "day": lhd.day,
            }  # "000{}-{:02d}-{:02d}".format(digit, lhd.month, lhd.day)

            lat = float(row[1])
            lon = float(row[2])
            prev_lat_lon = (lat, lon)
            prev_cs = cs

        ilr_seed_harvest_data[crop_id]["interpolate"] = NearestNDInterpolator(
            np.array(points), np.array(values)
        )
