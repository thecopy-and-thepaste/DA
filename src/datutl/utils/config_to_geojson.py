#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`

import traceback
import argparse
import json

from pathlib import Path
from shapely import Polygon
from shapely.ops import cascaded_union

from datutl.utils.log import get_logger

log = get_logger(__name__)


def convert(source_file: Path,
            dest_path: Path):
    try:
        dest_path.mkdir(parents=True, exist_ok=True)
        with open(source_file) as _f:
            config = json.load(_f)

        def feature_template(id, name, coordinates):
            template = {
                "type": "Feature",
                "properties": {
                        "ID": id,
                        "NAME": name
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [coordinates]
                }
            }

            return template

        
        projects = config["projects_info"]
        features = []

        for project in projects:
            locations = project["locations_info"]
            for location in locations:
                sites = location["sites_info"]

                tmp_polygons = []

                for site in sites:
                    site_id = site["site_id"]
                    desc = site["description"]
                    polygon = site["polygon"]
                    tmp_polygons.append(Polygon(polygon))

                
                res  = cascaded_union(tmp_polygons)
                polygons = [*res.geoms]
                

                for ix, pol in enumerate(polygons):
                    template = feature_template(ix, ix, 
                                                [*map(lambda x: list(x), [*zip(*pol.exterior.coords.xy)])])
                    features.append(template)


        res = {"type": "FeatureCollection",
               "features": features}

        with open(Path(dest_path) / "project_geojson.geojson", "w") as _f:
            json.dump(res, _f, indent=2)

    except Exception:
        log.exception(traceback.print_exc())
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
