'''
Downloading and extracting the MovieLense dataset
'''
import zipfile
import urllib.request
import os

def download_dataset():
    # Downloading movie lense dataset
    url = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip'
    urllib.request.urlretrieve(url, 'dataset.zip')


def extract_dataset():
    with zipfile.ZipFile('./dataset.zip', 'r') as zip_ref:
        zip_ref.extractall('.')

def delete_zipped_dataset_folder():
    os.unlink('./dataset.zip')

def main():
    # download dataset
    download_dataset()
    # extract dataset
    extract_dataset()
    # delete the zip files
    delete_zipped_dataset_folder()
    print('Dataset ready to be used')

if __name__ == '__main__':
    main()
