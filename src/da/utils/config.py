import multiprocessing


'''
FROM https://www.oreilly.com/library/view/python-cookbook/0596001673/ch05s23.html
'''
class Config(object):
    _num_workers = None
    _num_batches= None
    _shared_borg_state = {}
   
    def __new__(cls, *args, **kwargs):
        obj = super(Config, cls).__new__(cls, *args, **kwargs)
        obj.__dict__ = cls._shared_borg_state
        return obj

    @property
    def NUM_WORKERS(self):
        num_workers = self._num_workers

        if num_workers is None:
            num_workers = multiprocessing.cpu_count() - 1
            num_workers = 1 if num_workers < 1 else num_workers

        return num_workers

    @property
    def NUM_BATCHES(self):
        num_batches = self._num_batches

        if num_batches is None:
            num_batches = multiprocessing.cpu_count() - 1
            num_batches = 1 if num_batches < 1 else num_batches

        return num_batches