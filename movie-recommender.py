import codecs
import sys
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

    def recommendation(self, id):
        model = self.apply_ALS()
        userSchema = StructType([StructField('userID', IntegerType(), True)])
        users = self.spark.createDataFrame([[id,]], userSchema)
        return model.recommendForUserSubset(users, 10).collect()


def main(id):
    names = MovieRecommender().load_movie_names()
    recommendations = MovieRecommender().recommendation(id)
    for userRecs in recommendations:
        myRecs = userRecs[1]
        for rec in myRecs:
            movie = rec[0]
            rating = rec[1]
            movieName = names[movie]
            print(movieName + str(rating))

if __name__ == '__main__':
    id = int(sys.argv[1])
    main(id)


