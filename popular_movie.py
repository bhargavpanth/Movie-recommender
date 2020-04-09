from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def load_movie_names():
    movieNames = {}
    with codecs.open('./ml-100k/u.ITEM', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

class PopularMovie:
    def __init__(self):
        self.spark = SparkSession.builder.appName('popular_movie').getOrCreate()
        self.name_dict = self.spark.sparkContext.broadcast(load_movie_names())
        
    def look_up_name(self, movie_id):
        return self.name_dict.value[movie_id]

    def schema(self):
        return StructType([ \
                StructField('userID', IntegerType(), True), \
                StructField('movieID', IntegerType(), True), \
                StructField('rating', IntegerType(), True), \
                StructField('timestamp', LongType(), True) \
            ])

    def data_frame(self):
        schema = self.schema()
        return self.spark.read.option('sep', '\t').schema(schema).csv('./ml-100k/u.data')
