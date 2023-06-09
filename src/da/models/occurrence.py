#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`
from copy import deepcopy
import traceback
import pendulum

from datetime import datetime

from typing import Optional, Union

from shapely.geometry import Point, mapping

from pydantic import root_validator, validator

from da.models.basic import BaseBuilder as BB, IDiedModel

from da.utils.log import get_logger
log = get_logger(__name__)

class EventBuilder(BB):
    """Defines the interface to create the Event Model
    """
    class Model(IDiedModel):
        """The common model of the Event 

        Parameters
        ----------
        IDiedModel : IDiedModel
            If the ID is not provided. IDied will create one unique
        """
        latitude: float
        longitude: float
        eventType: str = "Catalog"

        coordinates: Point

        eventDate: Optional[datetime]

        class Config:
            arbitrary_types_allowed = True
    
        @root_validator(pre=True)
        def create_missing_values(cls, values: dict) -> dict:
            """Creates the missing coordinates Point using
            longitude and laitutde

            Parameters
            ----------
            values : dict
                Values of the model

            Returns
            -------
            dict
                Verified data
            """
            try:
                coords = Point(float(values['longitude']),
                               float(values['latitude']))
                values['coordinates'] = coords

                return values
            except Exception:
                log.exception(traceback.print_exc())
                raise

        @validator('eventDate', pre=True, always=True)
        def create_event_date(cls, value) -> datetime:
            """Verifies the eventDate. If it cames from string parses only to date

            Parameters
            ----------
            value : str | data
                Value to verify

            Returns
            -------
            datetime
                Date formatted
            """
            try:
                if value is None:
                    return
                
                if isinstance(value, str):
                    value = value.strip()

                    if len(value) == 0:
                        value = None
                    else:
                        value = pendulum.parse(value)

                return value
            except Exception:
                log.exception(traceback.print_exc())
                raise

        def dict(self) -> dict:
            """Returns the serializable version of the data

            Returns
            -------
            dict
                Serializable version
            """
            try:
                tmp = deepcopy(self.__dict__)
                coords = mapping(tmp['coordinates'])['coordinates']

                tmp['coordinates'] = {
                    'latitude': coords[1],
                    'longitude': coords[0]
                }
                tmp['eventID'] = tmp['id']
                del tmp['id']
                return tmp
            except Exception:
                log.exception(traceback.print_exc())
                raise

    @classmethod
    def transform(cls, data: dict) -> Model:
        """Creates the Model

        Parameters
        ----------
        data : dict
            Data to create the Event model

        Returns
        -------
        Model
            Typed Occurrence
        """
        try:
            data_typed = EventBuilder.Model(**data)

            return data_typed
        except Exception:
            raise

class OccurrenceBuilder(BB):
    """Defines the interface to create the Occurrence Model
    """
    class Model(IDiedModel):
        """The common model of the Occurrence 

        Parameters
        ----------
        IDiedModel : IDiedModel
            If the ID is not provided. IDied will create one unique
        """
        occurrenceID: str
        event: EventBuilder.Model
        verbatimID: Optional[Union[str, int]]
        verbatimSource: str

        class Config:
            arbitrary_types_allowed = True
        
        def dict(self) -> dict:
            """This returns the plain version of the Occurrence with
            the data of the Model

            Returns
            -------
            dict
                Serializable version
            """
            try:
                tmp = deepcopy(self.__dict__)
                event_dict = self.event.dict()

                del tmp['event']
                tmp.update(event_dict)

                return tmp
            except Exception:
                log.exception(traceback.print_exc())
                raise

    @classmethod
    def transform(cls, data: dict) -> Model:
        """Creates the Model

        Parameters
        ----------
        data : dict
            Data to create the model

        Returns
        -------
        Model
            Typed Occurrence
        """
        try:
            data_typed = OccurrenceBuilder.Model(**data)

            return data_typed
        except Exception:
            raise
