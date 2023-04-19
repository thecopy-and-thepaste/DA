"""

I can't move, and I don't want to
"""
from abc import ABC, abstractmethod
from typing import List

from uuid import uuid4

from pydantic import BaseModel
from pydantic.class_validators import root_validator

from datutl.utils.log import get_logger

log = get_logger(__name__)

class IDiedModel(BaseModel):
    id: str
    
    __ALL_IDS__: set = set()

    @classmethod
    def create_id(cls) -> str:
        """
        Creates a unique uuid ID

        Returns
        -------
        str
            String of the uuid unique ID.
        """
        tmp_id = str(uuid4())
        if tmp_id in cls.__ALL_IDS__:
            tmp_id = cls.create_id()

        cls.__ALL_IDS__.add(tmp_id)

        return tmp_id

    @root_validator(pre=True)
    def create_id_if_not(cls, values: dict) -> dict:
        """
        If we do not find the ID we create it

        Parameters
        ----------
        values : dict
            Values from the raw cat

        Returns
        -------
        dict
            Same object with modified keys
        """
        if 'id' not in values:
            values['id'] = cls.create_id()

        return values

class BaseBuilder(ABC):
    
    @classmethod
    @abstractmethod
    def transform(cls, data:dict):
        """_summary_

        Parameters
        ----------
        data : dict
            _description_
        """