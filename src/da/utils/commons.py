#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Remember to update the PYTHON_PATH to
# export PYTHONPATH=`pwd`:`pwd`

import traceback

import json

import collections
import multiprocessing
import traceback

from threading import Lock

from tqdm import tqdm
from typing import Callable, List
from concurrent.futures import ThreadPoolExecutor
from da.utils.config import Config

from da.utils.log import get_logger

log = get_logger(__name__)

LOCK = Lock()

# region general utils


def barified(func: Callable,
             data: collections,
             *args,
             **kwargs) -> List:
    """
    Ordered version to parallelize tasks using PoolExecutor and provide a bar
    for estimated time of completion.

    The data is splitted in items and send to a function with signature
    func(item, *args, **kwargs).


    Parameters
    ----------
    func : Callable
        Function to execute for each item in the data collection
    data : collections.Sized
        Data to process, should contains the len method (collections.Sized)

    Returns
    -------
    [List]
        [List of the results of func function]
    """
    try:
        total = 0

        if not hasattr(data, '__len__'):
            log.warning("Total size cannot be calculated at barified")
        else:
            total = len(data)

        
        max_workers = Config().NUM_WORKERS
        hide_bar = kwargs.get('hide_bar', False)
        processes_results = []

        with tqdm(total=total, disable=hide_bar) as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                try:
                    for result in executor.map(lambda x: func(x, *args, **kwargs), data):
                        processes_results.append(result)
                        pbar.update(1)
                except Exception:
                    log.exception(traceback.print_exc())
                    raise

        return processes_results
    except Exception:
        raise

def batchify(func: Callable,
             data: collections,
             *args,
             **kwargs) -> List:
    try:
        num_batches = kwargs.get('num_batches')
        batch_size = kwargs.get('batch_size')

        if num_batches is None:
            num_batches = Config().NUM_BATCHES
        
        if batch_size is None:
            batch_size = int(len(data) / num_batches) + 1

        batch_ixs = range(0, len(data), batch_size)
        batch_ixs = [*map(lambda x: (x, x + batch_size), batch_ixs)]

        kwargs['max_workers'] = Config().NUM_WORKERS

        tmp = barified(func, 
                       batch_ixs,
                       data,
                       *args,
                       **kwargs)
        

        if tmp is not None and isinstance(tmp, list):
            tmp = filter(lambda x: x is not None, tmp)
            res = [x for xx in tmp for x in xx]

            return res
    except Exception:
        log.exception(traceback.print_exc())
        raise
    
# endregion
