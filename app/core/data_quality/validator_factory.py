# core/data_quality/validator_factory.py
from .csv_validator import CSVValidator
from .json_validator import JSONValidator
import pandas as pd
from typing import Union, List, Dict
from .base_validator import BaseValidator
from .file_loader import load_file
from pathlib import Path


class ValidatorFactory:
    @staticmethod
    def create_validator(data_got_path: str) -> BaseValidator:
        # raise ValueError("CSV validator requires a pandas DataFrame")

        if data_got_path.endswith(".csv"):
            file_type = "csv"
        elif data_got_path.endswith(".json"):
            file_type = "json"
        else:
            raise ValueError(f"Unsupported data type: {data_got_path}")            

        data = load_file(data_got_path,file_type)

        if file_type == "csv":
            if not isinstance(data, pd.DataFrame):
                raise ValueError("CSV validator requires a pandas DataFrame")
            return CSVValidator(data)
        elif file_type == "json":
            if not isinstance(data, list):
                raise ValueError("JSON validator requires a list of dictionaries")
            return JSONValidator(data)
        else:
            raise ValueError(f"Unsupported data type: {file_type}")