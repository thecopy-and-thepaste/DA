import multiprocessing


'''
FROM https://www.oreilly.com/library/view/python-cookbook/0596001673/ch05s23.html
'''


class Config(object):
    """Config class to share values across different classes
    """
    _num_workers = None
    _num_batches = None
    _shared_borg_state = {}

    def __new__(cls, *args, **kwargs):
        obj = super(Config, cls).__new__(cls)
        obj.__dict__ = cls._shared_borg_state
        return obj

    @property
    def NUM_WORKERS(self) -> int:
        """Num of workers to parelelize

        Returns
        -------
        int
            Number of workers
        """
        num_workers = self._num_workers

        if num_workers is None:
            num_workers = max(multiprocessing.cpu_count() - 1, 1)

        return num_workers

    @property
    def NUM_BATCHES(self) -> int:
        """Number of batches to split in batchify

        Returns
        -------
        int
            Number of batches
        """
        num_batches = self._num_batches

        if num_batches is None:
            num_batches = max(multiprocessing.cpu_count() - 1, 1)

        return num_batches
