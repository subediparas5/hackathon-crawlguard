import pandas as pd
import json
from typing import Union, List, Dict

def load_file(file_path: str, file_type: str) -> Union[pd.DataFrame, List[Dict]]:
    if file_type == "csv":
        return pd.read_csv(file_path)
    elif file_type == "json":
        with open(file_path) as f:
            return json.load(f)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
