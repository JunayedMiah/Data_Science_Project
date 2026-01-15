import os
from kaggle.api.kaggle_api_extended import KaggleApi

RAW_DATA_PATH = "data/raw/fitbit"

def download_fitbit_data():
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    api = KaggleApi()
    api.authenticate()
    
    api.dataset_download_files(
        dataset= "arashnic/fitbit",
        path = RAW_DATA_PATH,
        unzip= True
    )
    
    print("Fitbit data download successfully")
    

if __name__ == "__main__":
    download_fitbit_data()