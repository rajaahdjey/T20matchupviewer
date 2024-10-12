import zipfile
import os
import json
from pathlib import Path

THIS_DIR = Path(__file__).parent

data_source = THIS_DIR / "./data_sources.json"

with open(data_source,'r') as file:
    DOWNLOAD_LIST = json.load(file)

def extract_zip(zip_file_loc,extract_loc):
    with zipfile.ZipFile(zip_file_loc,'r') as zip_f:
        zip_f.extractall(extract_loc)

for key,_ in DOWNLOAD_LIST.items():
    extract_zip(str(THIS_DIR) + fr"\\zip_downloads\\{key}.zip",str(THIS_DIR) + fr"\\extracted_data\\{key}")

