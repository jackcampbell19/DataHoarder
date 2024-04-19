from typing import Dict, List
import os
import boto3


s3 = boto3.client('s3')


ICON: str = """
 _____   ______  ______  ______  __  __  ______  ______  ______  _____   ______  ______    
/\\  __-./\\  __ \\/\\__  _\\/\\  __ \\/\\ \\_\\ \\/\\  __ \\/\\  __ \\/\\  == \\/\\  __-./\\  ___\\/\\  == \\   
\\ \\ \\/\\ \\ \\  __ \\/_/\\ \\/\\ \\  __ \\ \\  __ \\ \\ \\/\\ \\ \\  __ \\ \\  __<\\ \\ \\/\\ \\ \\  __\\\\ \\  __<   
 \\ \\____-\\ \\_\\ \\_\\ \\ \\_\\ \\ \\_\\ \\_\\ \\_\\ \\_\\ \\_____\\ \\_\\ \\_\\ \\_\\ \\_\\ \\____-\\ \\_____\\ \\_\\ \\_\\ 
  \\/____/ \\/_/\\/_/  \\/_/  \\/_/\\/_/\\/_/\\/_/\\/_____/\\/_/\\/_/\\/_/ /_/\\/____/ \\/_____/\\/_/ /_/ 
"""
CACHE_DIR: str = os.path.dirname(os.path.abspath(__file__)) + '/cache'
STUDY_PATH: str = 'study-1.0/raw-data/'
BUCKET: str = 'smart-mask-old'


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def select(message: str, options: Dict):
    keys: List[str] = list(options.keys())
    print(f"{message}:")
    for i in range(len(keys)):
        print(f"    [{i}] {keys[i]}")
    print()
    user_input: str = input(f"Selection: ")
    if not user_input.isnumeric() or int(user_input) not in range(len(keys)):
        clear()
        print("Invalid input, please try again.\n")
        select(message, options)
        return
    user_input_num: int = int(user_input)
    options[keys[user_input_num]](keys[user_input_num])


def query_folders(suffix: str = ''):
    bucket_name = BUCKET
    prefix = STUDY_PATH + suffix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    sub_folders: List[str] = []
    if 'CommonPrefixes' in response:
        for obj in response['CommonPrefixes']:
            split = list(filter(lambda x: x != '', obj['Prefix'].split('/')))
            sub_folders.append(split[-1])
    return sub_folders


def check_data(_: str):
    clear()
    print('Functionality not available yet.\n')


def download_date(suffix: str):
    clear()
    destination_path = os.path.join(CACHE_DIR, BUCKET, STUDY_PATH, suffix)
    print(destination_path)
    os.makedirs(destination_path, exist_ok=True)
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=STUDY_PATH + suffix)
    if 'Contents' in response:
        contents = response['Contents']
        for i in range(len(contents)):
            obj = contents[i]
            clear()
            print(f"Downloading \"{suffix}\" ({i + 1}/{len(contents)})\n")
            key = obj['Key']
            file_name = os.path.basename(key)
            local_file_path = os.path.join(destination_path, file_name)
            s3.download_file(BUCKET, key, local_file_path)
        clear()
        print(f"{len(contents)} files downloaded at path:\n{destination_path}\n")


def download_patient(patient: str):
    clear()
    date_folders: List[str] = query_folders(f"{patient}/")
    options = {}

    def wrapper(s: str):
        download_date(f"{patient}/{s}/")

    select(f"Pick a date to download data from \"{patient}\"",
           {date_folder: wrapper for date_folder in date_folders})


def download_data(_: str):
    clear()
    patient_folders: List[str] = query_folders()
    select('Pick a patient to download data for', {patient: download_patient for patient in patient_folders})


def entry():
    select(
        'Please select a mode', {
            'View / Delete': check_data,
            'Download': download_data
        }
    )


if __name__ == '__main__':
    print(ICON)
    entry()
