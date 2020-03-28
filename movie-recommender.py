import codecs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS

class MovieRecommender:
    def __init__(self):
        self.dataset_folder = './ml-100k/'
        self.item = self.dataset_folder + 'u.item'
        self.data = self.dataset_folder + 'u.data'
        self.spark = SparkSession.builder.appName('movie_recommender').getOrCreate()

    def load_movie_names(self):
        movieNames = {}
        with codecs.open(self.item, 'r', encoding='ISO-8859-1', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                movieNames[int(fields[0])] = fields[1]
        return movieNames

    def schema(self):
        return StructType([ \
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True) \
                ])

    def read_ratings(self):
        movie_schema = self.schema()
        ratings = self.spark.read.option('sep', '\t').schema(movie_schema)
        print(ratings.schema)
        return ratings

    def apply_ALS(self):
        ratings = self.read_ratings()
        als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol('userID').setItemCol('movieID').setRatingCol('rating')
        return als.fit(ratings)

    def recommendation(self):
        model = self.read_ratings()
        

