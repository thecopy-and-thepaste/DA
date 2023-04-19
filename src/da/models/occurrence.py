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

from datutl.models.basic import BaseBuilder as BB, IDiedModel

from datutl.utils.log import get_logger
log = get_logger(__name__)

class EventBuilder(BB):
    class Model(IDiedModel):
        latitude: float
        longitude: float
        eventType: str = "Catalog"

        coordinates: Point

        eventDate: Optional[datetime]

        class Config:
            arbitrary_types_allowed = True
    
        @root_validator(pre=True)
        def create_missing_values(cls, values: dict) -> dict:
            try:
                coords = Point(float(values['longitude']),
                               float(values['latitude']))
                values['coordinates'] = coords

                return values
            except Exception:
                log.error(traceback.print_exc())
                raise

        @validator('eventDate', pre=True, always=True)
        def create_event_date(cls, value):
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
                log.error(traceback.print_exc())
                breakpoint()
                raise

        def dict(self):
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
                log.error(traceback.print_exc())
                raise

    @classmethod
    def transform(cls, data: dict):
        try:
            data_typed = EventBuilder.Model(**data)

            return data_typed
        except Exception:
            raise

class OccurrenceBuilder(BB):
    class Model(IDiedModel):
        occurrenceID: str
        event: EventBuilder.Model
        verbatimID: Optional[Union[str, int]]
        verbatimSource: str

        class Config:
            arbitrary_types_allowed = True
        
        def dict(self):
            try:
                tmp = deepcopy(self.__dict__)
                event_dict = self.event.dict()

                del tmp['event']
                tmp.update(event_dict)

                return tmp
            except Exception:
                log.error(traceback.print_exc())
                raise

    @classmethod
    def transform(cls, data: dict):
        try:
            data_typed = OccurrenceBuilder.Model(**data)

            return data_typed
        except Exception:
            raise
