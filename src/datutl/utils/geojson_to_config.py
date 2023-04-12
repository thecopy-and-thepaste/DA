#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`

import traceback
import argparse
import json

import geopandas as gp

from pathlib import Path
from shapely import MultiPolygon, Polygon

from datutl.utils.log import get_logger

log = get_logger(__name__)

def id_gen():
    acc = 0

    def wrap():
        nonlocal acc
        acc += 1
        return acc

    return wrap

gen = id_gen()

def convert(source_file: Path,
            dest_path: Path):
    try:
        def wrapper(site_row):
            try:
                temp = site_row.geometry

                if isinstance(temp, MultiPolygon):
                    xmin, ymin, xmax, ymax = temp.bounds
                    bb = Polygon([(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)])\
                        .buffer(1, resolution=1)
                    polygon = bb.exterior.coords
                else:
                    polygon = temp.exterior.coords

                site_template = {
                    "site_id": site_row.get("ID", gen()),
                    "polygon": list(polygon),
                    "desc": site_row.get("NAME", "")
                }

                return site_template
            except Exception:
                log.error(traceback.print_exc())
                raise

        df = gp.read_file(source_file)
        sites = []
        for _, row in df.iterrows():
            site = wrapper(row)
            sites.append(site)
        
        project_info = {
            "projects_info": [
                {
                    'project_id': "Zambia",
                    "locations_info": [
                        {
                            "location_id": "location_1",
                            "location_info": {
                                "location_description": "Descripcion del sitio"
                            },
                            "sites_info": sites
                        }
                    ]
                }
            ]
        }

        with open(dest_path / "project_info.json", "w") as _f:
            json.dump(project_info, _f, indent=2)

    except Exception:
        log.error(traceback.print_exc())
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-sf', '--source_file',
                        help='Config file that contains the collections to query',
                        required=True)
    parser.add_argument('-dp', '--dest_path',
                        help='Destination path for the pipeline assets',
                        required=True)

    ARGS = parser.parse_args()

    source_file = Path(ARGS.source_file)
    dest_path = Path(ARGS.dest_path)

    assert source_file.exists(), 'Config file does not exist'

    if not dest_path.exists():
        log.warning('Destination path does not exist. Making ...')
        dest_path.mkdir(parents=True)

    convert(source_file=source_file,
            dest_path=dest_path)
