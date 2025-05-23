#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import argparse
import asyncio
import capnp
import os
import sys
import tomlkit as tk
import uuid
from zalfmas_common.climate import common_climate_data_capnp_impl as ccdi
from zalfmas_common import common
from zalfmas_common import service as serv
from zalfmas_common.climate import csv_file_based as csv_based
import zalfmas_capnp_schemas
sys.path.append(os.path.dirname(zalfmas_capnp_schemas.__file__))
import climate_capnp
import registry_capnp as reg_capnp


def create_meta_plus_datasets(path_to_data_dir, interpolator, rowcol_to_latlon, restorer):
    datasets = []
    metadata = climate_capnp.Metadata.new_message(
        entries=[
            {"historical": None},
            {"start": {"year": 1901, "month": 1, "day": 1}},
            {"end": {"year": 2022, "month": 9, "day": 30}}
        ]
    )
    metadata.info = ccdi.MetadataInfo(metadata)
    transform_map = {
        "globrad": lambda gr: gr / 1000.0 if gr > 0 else gr
    }
    if "germany_ubn_1901-01-01_to_2022-09-30" in path_to_data_dir:
        transform_map["relhumid"] = lambda rh: rh * 100.0

    datasets.append(climate_capnp.MetaPlusData.new_message(
        meta=metadata,
        data=csv_based.Dataset(metadata, path_to_data_dir, interpolator, rowcol_to_latlon,
                               header_map={
                                   "Date": "iso-date",
                                   "Precipitation": "precip",
                                   "TempMin": "tmin",
                                   "TempMean": "tavg",
                                   "TempMax": "tmax",
                                   "Radiation": "globrad",
                                   "Windspeed": "wind",
                                   "RelHumCalc": "relhumid"
                               },
                               supported_headers=["tmin", "tavg", "tmax", "precip", "globrad", "wind", "relhumid"],
                               row_col_pattern="{row}/daily_mean_RES1_C{col}R{row}.csv.gz",
                               pandas_csv_config={"skiprows": 0, "sep": "\t"},
                               transform_map=transform_map,
                               restorer=restorer)
    ))
    return datasets


default_config = {
    "id": str(uuid.uuid4()),
    "name": "DWD/UBN - historical - 1901 - 2023",
    "description": None,
    "path_to_data": "path to data here",
    "path_to_latlon_to_rowcol": "path to latlon_to_rowcol.json here",
    "host": None,
    "port": None,
    "serve_bootstrap": True,
    "fixed_sturdy_ref_token": None,
    "reg_sturdy_ref": None,
    "reg_category": None,

    "opt:id": "ID of the service",
    "opt:name": "DWD/UBN - historical - 1901 - 2023",
    "opt:description": "Description of the service",
    "opt:path_to_data": "[string (path)] -> Path to the directory containing the data (rows)",
    "opt:path_to_latlon_to_rowcol": "[string (path)] -> Path to the JSON file containing the lat/lon to row/coll mapping",
    "opt:host": "[string (IP/hostname)] -> Use this host (e.g. localhost)",
    "opt:port": "[int] -> Use this port (missing = default = choose random free port)",
    "opt:serve_bootstrap": "[true | false] -> Is the service reachable directly via its restorer interface",
    "opt:fixed_sturdy_ref_token": "[string] -> Use this token as the sturdy ref token of this service",
    "opt:reg_sturdy_ref": "[string (sturdy ref)] -> Connect to registry using this sturdy ref",
    "opt:reg_category": "[string] -> Connect to registry using this category",
}
async def main():
    parser = serv.create_default_args_parser("DWD/UBN - historical")
    config, args = serv.handle_default_service_args(parser, default_config)

    path_to_data = config["path_to_data"]
    path_to_latlon_to_rowcol = config["path_to_latlon_to_rowcol"]

    restorer = common.Restorer()
    interpolator, rowcol_to_latlon = ccdi.create_lat_lon_interpolator_from_json_coords_file(
        os.path.join(path_to_data, path_to_latlon_to_rowcol))
    meta_plus_data = create_meta_plus_datasets(path_to_data, interpolator, rowcol_to_latlon, restorer)
    service = ccdi.Service(meta_plus_data, id=config["id"], name=config["name"], description=config["description"],
                           restorer=restorer)
    await serv.init_and_run_service({"service": service},
                                    config["host"], config["port"],
                                    serve_bootstrap=config["serve_bootstrap"],
                                    name_to_service_srs={"service": config["fixed_sturdy_ref_token"]},
                                    restorer=restorer)

if __name__ == '__main__':
    asyncio.run(capnp.run(main()))
