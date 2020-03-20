'''
Downloading and extracting the MovieLense dataset
'''
import urllib.request

def download_dataset():
    # Downloading movie lense dataset
    url = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip'
    urllib.request.urlretrieve(url, 'dataset')

def download_checksum():
    # Downloading dataset checksum
    checksum_url = 'http://files.grouplens.org/datasets/movielens/ml-100k.zip.md5'
    urllib.request.urlretrieve(checksum_url, 'checksum')

def extract_dataset():
    pass


def delete_zipped_dataset_folder():
    pass

def main():
    # download dataset
    # download checksum
    # extract dataset
    # read contents from checksum file and check the checksum of the extracted dataset
    pass

if __name__ == '__main__':
    main()
