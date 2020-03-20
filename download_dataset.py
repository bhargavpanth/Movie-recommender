'''
Downloading and extracting the MovieLense dataset
'''
import zipfile
import urllib.request

def download_dataset():
    # Downloading movie lense dataset
    url = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip'
    urllib.request.urlretrieve(url, 'dataset.zip')

def download_checksum():
    # Downloading dataset checksum
    checksum_url = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip.md5'
    urllib.request.urlretrieve(checksum_url, 'checksum.md5')

def extract_dataset():
    with zipfile.ZipFile('./dataset.zip', 'r') as zip_ref:
        zip_ref.extractall('.')

def delete_zipped_dataset_folder():
    pass

def main():
    download_dataset()
    download_checksum()
    extract_dataset()
    # download dataset
    # download checksum
    # extract dataset
    # read contents from checksum file and check the checksum of the extracted dataset
    pass

if __name__ == '__main__':
    main()
