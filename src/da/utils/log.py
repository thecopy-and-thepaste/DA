import os
import sys
import logging
import tempfile
import platform

from dotenv import load_dotenv
load_dotenv()



# Verify if system is macOS
foldername = '/tmp' if platform.system() == 'Darwin' else tempfile.gettempdir()
foldername = os.path.join(foldername, 'logs')
if not os.path.exists(foldername):
    os.makedirs(foldername)

def_log_name = __file__.split("/")[-3:-1]
def_log_name = f'{"_".join(def_log_name)}_pkcg'

LOG_NAME = os.environ.get("LOG_NAME", def_log_name)
FILENAME = f"{LOG_NAME}.log"
PATH = os.path.join(foldername, FILENAME)
FORMAT = "%(asctime)s [%(name)-12s] [%(levelname)-5.5s]  %(message)s"
DEFAULT_LEVEL = logging.INFO
logFormatter = logging.Formatter(FORMAT)
logging.basicConfig(stream=sys.stderr, format=FORMAT)


def get_logger(name, path=PATH, level=DEFAULT_LEVEL):
    """Return a logger with the specified name.

    Parameters
    ----------
    name : str
        Name of the logger
    path : str, optional
        Path of the file for logging (default is PATH)
    level : int or str, optional
        Logging level of this logger (default is DEFAULT_LEVEL)

    Returns
    -------
    Logger
        Instance of a logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    filename = path
    fileHandler = logging.FileHandler(filename, mode='a')
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    return logger