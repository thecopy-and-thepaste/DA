#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`

import argparse
import traceback

import pandas as pd

from da.models.occurrence import EventBuilder as EB, OccurrenceBuilder as OB
from da.models.taxon import TaxonBuilder as TB

from pathlib import Path
from da.utils.log import get_logger

log = get_logger(__name__)


def sample_conversion(sample_file: Path):
    try:
        df = pd.read_csv(sample_file)


        # VerbatimID is mandatory
        df = df[~df.verbatim_id.isnull()]
        df['eventDate'] = df['eventDate'].fillna('')

        taxa = []
        occurrences = []
        events = []

        # Simplest scenario
        for _, row in df.iterrows():
            # By default, event_type is catalog
            tmp_event = {
                'latitude': row.latitude,
                'longitude': row.longitude,
                'eventDate': row.eventDate
            }
            event = EB.transform(tmp_event)

            tmp_occurrence = {
                'occurrenceID': int(row.occurrence_id),
                'event': event,
                'verbatimID': int(row.verbatim_id),
                'verbatimSource': row.verbatim_source
            }

            occurrence = OB.transform(tmp_occurrence)
            occurrences.append(occurrence)

            tmp_taxon = {
                'kingdom': row.kingdom,
                'phylum': row.phylum,
                'class_taxon': row.class_taxon,
                'order': row.order,
                'family': row.family,
                'subfamily': row.latitude,
                'genus': row.subfamily,
                'species': row.species,

                'scientificName': row.scientific_name,
                'canonicalName': row.canonicalName,
                'verbatimID': row.verbatim_id,
                'verbatimSource': row.verbatim_source
            }

            taxon = TB.transform(tmp_taxon)
            taxa.append(taxon)

        return occurrences, taxa

    except Exception:
        log.exception(traceback.print_exc())
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-sf', '--sample_file',
                        help='Sample file to convert to data model',
                        required=True)
    parser.add_argument('-dp', '--dest_path',
                        help='Destination path for the pipeline assets',
                        required=True)

    ARGS = parser.parse_args()

    sample_file = Path(ARGS.sample_file)
    dest_path = Path(ARGS.dest_path)

    assert sample_file.exists(), 'Sample file does not exist'
    assert sample_file.is_file(), 'Sample file is not a file'

    if not dest_path.exists():
        log.warning('Destination path does not exist. Making ...')
        dest_path.mkdir(parents=True)

    occurrences, taxa = sample_conversion(sample_file=sample_file)
    csv_path = (dest_path / "csv")

    csv_path.mkdir(exist_ok=True, parents=True)

    occurrences = [*map(lambda x: x.dict(), occurrences)]
    taxa = [*map(lambda x: x.dict(), taxa)]

    # RUN sample vía docker
    # docker run -it --rm -v /PATH/TO/HOST:/home/sources -v /PATH/TO/HOST:/home/results da python examples -sf /home/sources/file_name.csv -dp /home/results

    pd.DataFrame(occurrences).to_csv(csv_path / "occurrences.csv")
    pd.DataFrame(taxa).to_csv(csv_path / "taxa.csv")
