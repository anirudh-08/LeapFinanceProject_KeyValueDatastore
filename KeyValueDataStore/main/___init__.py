import os
import fcntl

from . import config
from .data_store import DataStore


def get_file_name() -> str:
    """
    To create a new file name
    """
    import uuid
    uniq_append_string = uuid.uuid4().hex
    return "LOCAL_STORAGE_{}".format(uniq_append_string)


def get_instance(file_name=None) -> DataStore:
    if file_name is None:
        file_name = get_file_name()
    full_file_name = f"{config.LOCAL_STORAGE_PREPEND_PATH}/{file_name}"
    file_descriptor = os.open(full_file_name, os.O_CREAT | os.O_RDWR)

    """
        Try to acquire file lock. 
    """
    try:
        print(f"Acquiring file lock on {file_name}")
        fcntl.flock(file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise BlockingIOError(f"Resource '{file_name}' is already locked'")
    except Exception:
        raise
    else:
        print(f"File lock acquired on {file_name}")

    """
        File lock acquired.
    """
    if not os.path.isfile(full_file_name) or os.fstat(file_descriptor).st_size == 0:
        with open(full_file_name, 'ab') as f:
            string = "{}"
            f.write(bytes(string.encode('ascii')))
    return DataStore(file_descriptor)

__all__ = ['get_instance']