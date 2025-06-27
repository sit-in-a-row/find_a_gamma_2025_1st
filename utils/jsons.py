import json
import os
import pandas as pd

def load_json(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def save_json(obj, path: str):
    directory = os.path.dirname(path)
    if directory:                       
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_jsons_in_range(folder: str, START_DATE, END_DATE) -> pd.DataFrame:
    records = []
    for dt in pd.date_range(START_DATE, END_DATE, freq='D'):
        fn   = dt.strftime('%Y-%m-%d') + '.json'
        path = os.path.join(folder, fn)
        if os.path.exists(path):
            data = load_json(path)
            # list인지 dict인지 판별
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)
    return pd.DataFrame(records)
