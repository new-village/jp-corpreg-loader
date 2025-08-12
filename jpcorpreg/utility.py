""" Utility
It is utility functions class.
"""
import json
import os

import jpcorpreg


def load_config(data_type:str) -> str:
    """ The function loads configuration file from config directory
    :param data_type: Category is identifier of data types such as ENTRY, ODDS, RACE and RESULT.
    """
    try:
        dir_location = os.path.dirname(jpcorpreg.__file__) + '/config/'
        with open(dir_location + data_type + '.json', 'r', encoding='UTF-8') as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise SystemExit(f'Config file decode error: {exc}') from exc
    except FileNotFoundError as exc:
        raise SystemExit(f'Config file not found: {exc}') from exc
