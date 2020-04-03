import codecs
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS

class MovieRecommender:
    def __init__(self, id):
        self.id = id
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

    def recommendation(self):
        movie_schema = self.schema()
        ratings = self.spark.read.option('sep', '\t').schema(movie_schema)
        als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol('userID').setItemCol('movieID').setRatingCol('rating')
        model = als.fit(ratings)
        userSchema = StructType([StructField('userID', IntegerType(), True)])
        users = self.spark.createDataFrame([[self.id,]], userSchema)
        return model.recommendForUserSubset(users, 10).collect()

def main(id):
    names = MovieRecommender(id).load_movie_names()
    recommendations = MovieRecommender(id).recommendation()
    for userRecs in recommendations:
        myRecs = userRecs[1]
        for rec in myRecs:
            movie = rec[0]
            rating = rec[1]
            movieName = names[movie]
            print(movieName + str(rating))


if __name__ == '__main__':
    id = int(sys.argv[1])
    if id:
        main(id)
    else:
        print('Must pass an ID')
        exit(0)

