"""
- Creates 5 keys (a,b,c,d,e)
- For each key generates N entry random values
- The data has 10000 records
- Groups each key and sort the values
- For each key returns the element in index 1, 100, 500, 1000, 2000, 5000, 10000
- The scripts runs in distributed mode for each key
"""

from pyspark.sql import SparkSession
from random import randint

spark = SparkSession \
    .builder \
    .appName('distributed_key_processing') \
    .getOrCreate()

columns = ['key', 'value']
keys = ['a', 'b', 'c', 'd', 'e']
data = [(keys[randint(0, 4)], randint(1, 10000000)) for _ in range(10000)]

df = spark.createDataFrame(data, columns)

ranks = [1, 100, 500, 1000, 2000, 5000, 10000]


def mapWithIndex(rows):
    class MapWithIndex():
        def __init__(self) -> None:
            self.counter = 0

        def map(self, rows):
            sortedList = sorted(list(rows))
            result = []

            for element in sortedList:
                index = self.counter
                self.counter = self.counter + 1
                result.append((index, element))
            return result

    mapper = MapWithIndex()
    return mapper.map(rows)


def mapRankedValues(row):
    return [value for (key, value) in row if key in ranks]


df.rdd.groupByKey().mapValues(mapWithIndex).mapValues(mapRankedValues).collect()
