
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`
from typing import Optional, Union

from datutl.models.basic import BaseBuilder as BB, IDiedModel

from datutl.utils.log import get_logger
log = get_logger(__name__)


class BaseTaxonBuilder(BB):
    class Model(IDiedModel):
        kingdom: Optional[str]
        phylum: Optional[str]
        class_taxon: Optional[str]
        order: Optional[str]
        family: Optional[str]
        subfamily: Optional[str]
        genus: Optional[str]
        subgenus: Optional[str]
        species: Optional[str]

        scientificName: str
        canonicalName: str

        class Config:
            validate_assignment = True

    @classmethod
    def transform(cls, data: dict):
        try:
            data_typed = BaseTaxonBuilder.Model(**data)

            return data_typed
        except Exception:
            raise


class TaxonBuilder(BB):

    class Model(BaseTaxonBuilder.Model):
        verbatimID: Optional[Union[str, int]]
        verbatimSource: Optional[str]

        class Config:
            validate_assignment = True

    @classmethod
    def transform(cls, data: dict):
        try:
            data_typed = TaxonBuilder.Model(**data)

            return data_typed
        except Exception:
            raise
