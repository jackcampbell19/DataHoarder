import os
from typing import List, TextIO
from pathlib import Path

import boto3


COMPUTER_HOME_DIR: str = str(Path.home())
CACHE_ROOT_PATH: str = os.path.join(COMPUTER_HOME_DIR, '.dataHoarderCache')

s3 = boto3.client('s3')


class Cachable:

    def __init__(self, invalidate: bool):
        self._invalidate = invalidate
        self._local_cache_absolute_path: str = ''

    def _download_content(self):
        pass

    def name(self) -> str:
        pass


class CachedFile(Cachable):

    def __init__(self, invalidate: bool):
        super().__init__(invalidate)

    def get_file(self, mode: str) -> TextIO:
        pass

    def _get_cache_path(self) -> str:
        pass


class S3CachedFile(CachedFile):

    def __init__(self, bucket: str, path: str, filename: str, invalidate: bool = False):
        super().__init__(invalidate)
        self._bucket = bucket
        self._path = path
        self._filename = filename

    def _get_cache_path(self) -> str:
        return os.path.join(CACHE_ROOT_PATH, self._bucket, self._path, self._filename)

    def _download_content(self):
        cached_file_path: str = self._get_cache_path()
        s3_path: str = os.path.join(self._path, self._filename)
        if not self._invalidate and os.path.exists(cached_file_path):
            return
        # TODO: Check if the file exists in s3 before downloading
        os.makedirs(os.path.dirname(cached_file_path), exist_ok=True)
        s3.download_file(self._bucket, s3_path, cached_file_path)

    def get_file(self, mode: str) -> TextIO:
        self._download_content()
        return open(self._get_cache_path(), mode)

    def name(self) -> str:
        return os.path.join(self._bucket, self._path, self._filename)


class CachedFolder(Cachable):

    def __init__(self, invalidate: bool):
        super().__init__(invalidate)
        self._folders: List[CachedFolder] = []
        self._files: List[CachedFile] = []

    def get_files(self) -> List[CachedFile]:
        self._download_content()
        return self._files

    def get_folders(self) -> List['CachedFolder']:
        self._download_content()
        return self._folders


class S3CachedFolder(CachedFolder):

    def __init__(self, bucket: str, path: str, invalidate: bool = False):
        super().__init__(invalidate)
        self._bucket = bucket
        self._path = path

    def _download_content(self):
        response = s3.list_objects_v2(Bucket=self._bucket, Prefix=self._path + '/', Delimiter='/')
        sub_folders: List[str] = []
        if 'CommonPrefixes' in response:
            for obj in response['CommonPrefixes']:
                split = list(filter(lambda x: x != '', obj['Prefix'].split('/')))
                sub_folders.append(split[-1])
        for f in sub_folders:
            folder_path: str = os.path.join(self._path, f)
            self._folders.append(
                S3CachedFolder(self._bucket, folder_path, self._invalidate)
            )
        response = s3.list_objects_v2(Bucket=self._bucket, Prefix=self._path)
        if 'Contents' in response:
            contents = response['Contents']
            for i in range(len(contents)):
                obj = contents[i]
                key = obj['Key']
                file_name = os.path.basename(key)
                if str(os.path.join(self._path, file_name)) == key and file_name != '':
                    self._files.append(
                        S3CachedFile(self._bucket, self._path, file_name, self._invalidate)
                    )

    def name(self) -> str:
        return os.path.join(self._bucket, self._path)
