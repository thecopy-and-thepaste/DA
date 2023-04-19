
import os
import traceback
import multiprocessing

import pymongo

from threading import Lock
from datetime import datetime

from pydantic import BaseModel
from da.models.basic import BaseBuilder

from dotenv import load_dotenv

from da.utils.log import get_logger

load_dotenv()

log = get_logger(__name__)

NUM_PROCESSES = multiprocessing.cpu_count() - 2
LOCK = Lock()


class CachrModelBuilder(BaseBuilder):
    class Model(BaseModel):
        document_id: str
        updated_at: datetime
        document: dict

    @classmethod
    def transform(cls, data: dict):
        try:
            data_typed = CachrModelBuilder.Model(**data)

            return data_typed
        except Exception:
            raise


class Cachr:

    def __init__(self, key_collection: str) -> None:
        CACHR_CONNECTION = os.environ.get("CACHR_DB_CONNECTION")
        client = pymongo.MongoClient(CACHR_CONNECTION)
        self.__collection_name = key_collection

        self.__db = client["bed_cachr"]
        self.cachr_collection = self.__db[key_collection]

    def cach(self, ep: str, data: dict):
        try:
            doc = CachrModelBuilder.transform({
                "document_id": ep,
                "updated_at": datetime.now(),
                "document": data
            })

            with LOCK:
                self.cachr_collection.update_one(
                    {"document_id": doc.document_id},
                    {"$set": doc.__dict__},
                    upsert=True
                )

        except Exception:
            log.error(traceback.print_exc())
            raise

    def is_cachd(self, ep: str):
        try:
            docs = self.cachr_collection.find({
                "document_id": ep
            })
            docs = [*docs]


            if len(docs) == 1:
                return docs[0]
            elif len(docs) > 1:
                raise Exception(f"More than one document the endopint {ep}")

        except Exception:
            log.error(traceback.print_exc())
            raise
