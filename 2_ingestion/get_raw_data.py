#chương trình này là để lấy dữ liệu từ kaggle

import os
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

load_dotenv()

username = os.getenv("KAGGLE_USERNAME")
key = os.getenv("KAGGLE_KEY")

os.environ['KAGGLE_USERNAME'] = username
os.environ['KAGGLE_KEY'] = key

api = KaggleApi()
api.authenticate()

dataset_path = 'abhayayare/e-commerce-dataset' 
download_dir = './data'
os.makedirs(download_dir, exist_ok=True)

api.dataset_download_files(dataset_path, path=download_dir, unzip=True)
print("Download và giải nén xong!")