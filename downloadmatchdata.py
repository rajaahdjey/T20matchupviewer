import requests
import json

from pathlib import Path

THIS_DIR = Path(__file__).parent


data_source = THIS_DIR / "./data_sources.json"

with open(data_source,'r') as file:
    data_source_json = json.load(file)
    DOWNLOAD_LIST = data_source_json["match_data"]
    PEOPLE_LIST = data_source_json["aux_data"]


for key,link in DOWNLOAD_LIST.items():
    response = requests.get(link,timeout=3000)
    if response.status_code == 200:
        with open(str(THIS_DIR) + fr"\\zip_downloads\\{key}.zip", 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully")
    elif response.status_code == 404:
        print("Error 404 : File not found. Please check link.")
    else:
        print(f"Failed to download file due to status code : {response.status_code}")


#people data : 

for key,link in PEOPLE_LIST.items():
    response = requests.get(link,timeout=3000)
    if response.status_code == 200:
        with open(str(THIS_DIR) + fr"\\extracted_data\\{key}.csv", 'wb') as file:
            file.write(response.content)
        print("File downloaded successfully")
    elif response.status_code == 404:
        print("Error 404 : File not found. Please check link.")
    else:
        print(f"Failed to download file due to status code : {response.status_code}")