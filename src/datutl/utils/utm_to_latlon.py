#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`

import argparse
import traceback
import json
import numpy as np

import utm

from pathlib import Path
from datutl.utils.log import get_logger

log = get_logger(__name__)


def convert_coords(coords,
                   opts={
                       "zone_number": 15,
                       "zone_letter": "N"
                   }):
    try:
        for ix in range(len(coords)):
            coord = coords[ix]

            coord_len = len(np.array(coord).shape)

            if coord_len == 1:
                new_c = utm.to_latlon(coord[0], coord[1],
                                      opts['zone_number'],
                                      opts['zone_letter'])
                coords[ix] = list(new_c)[::-1]
            else:
                coords[ix] = convert_coords(coord)

        return coords

    except Exception:
        log.error(traceback.print_exc())
        raise


def to_lat_log(source_file: Path) -> dict:
    try:
        with open(source_file) as _f:
            src = json.load(_f)

        coord_features = src.get('features', {})

        for feat in coord_features:
            coords = feat.get('geometry', {}).get('coordinates', [])

            convert_coords(coords=coords)

        src['features'] = coord_features
        return src

    except Exception:
        log.error(traceback.print_exc())
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-sf', '--source_file',
                        help='Source file with utm coords',
                        required=True)

    ARGS = parser.parse_args()

    source_file = Path(ARGS.source_file)

    assert source_file.exists(), \
        "Source file does not exist"

    res = to_lat_log(source_file=source_file)

    dest_file = source_file.parent / \
        Path(f"{source_file.stem}_latlon{source_file.suffix}")

    with open(dest_file, mode='w') as _f:
        json.dump(res, _f, indent=2)
