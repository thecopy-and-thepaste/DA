
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`
from typing import Optional, Union

from da.models.basic import BaseBuilder as BB, IDiedModel

from da.utils.log import get_logger
log = get_logger(__name__)


class BaseTaxonBuilder(BB):
    """Defines the interface to create the BaseTaxon Model
    """
    class Model(IDiedModel):
        """The BaseTaxonModel provides the general information of the Taxon

        Parameters
        ----------
        IDiedModel : IDiedModel
            If the ID is not provided. IDied will create one unique
        """
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
    """Defines the interface to create the TaxonModel
    """

    class Model(BaseTaxonBuilder.Model):
        """The TaxonModel also contains the original ID and the source it provides
        """
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
