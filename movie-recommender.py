class MovieRecommender:
    def __init__(self):
        self.dataset_folder = './ml-100k/'
        self.item = self.dataset_folder + 'u.item'
        self.data = self.dataset_folder + 'u.data'

    def load_movie_names(self):
        pass
