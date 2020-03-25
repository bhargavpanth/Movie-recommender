import codecs

class MovieRecommender:
    def __init__(self):
        self.dataset_folder = './ml-100k/'
        self.item = self.dataset_folder + 'u.item'
        self.data = self.dataset_folder + 'u.data'

    def load_movie_names(self):
        movieNames = {}
        with codecs.open(self.item, 'r', encoding='ISO-8859-1', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                movieNames[int(fields[0])] = fields[1]
        return movieNames 
