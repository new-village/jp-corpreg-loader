""" Utility
It is utility functions class.
"""
import json
import os
from importlib import resources

import cnparser


def load_config(data_type:str) -> str:
    """ The function loads configuration file from config directory
    :param data_type: Category is identifier of data types such as ENTRY, ODDS, RACE and RESULT.
    """
    try:
        dir_location = os.path.dirname(cnparser.__file__) + '/config/'
        with open(dir_location + data_type + '.json', 'r', encoding='UTF-8') as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise SystemExit(f'Config file decode error: {exc}') from exc
    except FileNotFoundError as exc:
        raise SystemExit(f'Config file not found: {exc}') from exc

def load_api() -> str:
    """Return a file:// URL for the API resource if present.

    Note: This uses importlib.resources and does not require setuptools.
    If the resource does not exist, a FileNotFoundError is raised.
    """
    package = 'cnparser'
    rel_path = 'config/api/ja.json'
    # Prefer modern API if available (Python 3.9+)
    try:
        with resources.as_file(resources.files(package).joinpath(rel_path)) as p:
            if p.exists():
                return 'file://' + str(p).replace('.json', '')
    except (AttributeError, FileNotFoundError):
        # Fallback for older Python versions
        try:
            with resources.path(package, rel_path) as p:
                if p.exists():
                    return 'file://' + str(p).replace('.json', '')
        except FileNotFoundError:
            pass
    raise FileNotFoundError(f"No such file or directory: '{rel_path}'")