#import pyspark referenced from: http://renien.com/blog/accessing-pyspark-pycharm/
#!/usr/bin/env python3
import os
import sys


os.environ['SPARK_HOME']="/Users/liulei/mie1628_spark/spark-3.5.1-bin-hadoop3"
sys.path.append("/Users/liulei/mie1628_spark/spark-3.5.1-bin-hadoop3/python/")
sys.path.append ("/Users/liulei/Library/Python/3.8/lib/python/site-packages/")
import numpy as np

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    print("Successfully imported Spark module!")

except ImportError as e:
    print("Error importing Spark module", e)
    sys.exit(1)

#create the spark session
spark = SparkSession.builder.appName("ass2").getOrCreate()

########## PartA Q1 ##########
#load textfile and create integer RDD
sc = spark.sparkContext
integer_rdd = sc.textFile("/Users/liulei/Desktop/mie1628/ass2/integer.txt")
integer_rdd = integer_rdd.map(lambda x: int(x))

#filter it with even and odd
even = integer_rdd.filter(lambda x: x % 2 == 0)
even_count =  even.count()
odd = integer_rdd.filter(lambda x: x % 2 != 0)
odd_count = odd.count()

print("---------- PartA Q1 Answer ----------")
print("odd numbers are: {" + str(odd_count) + "} and even numbers are {" + str(even_count) + "}")
print("-------------------------------------")


########## PartA Q2 ##########
salary_rdd = sc.textFile("/Users/liulei/Desktop/mie1628/ass2/salary.txt")
#format rdd as Key-Value Pairs: str, int
salary_rdd = salary_rdd.map(lambda x: x.split(" "))
salary_rdd = salary_rdd.map(lambda x: (x[0], int(x[1])))
#reduce by key
salary_sum = salary_rdd.reduceByKey(lambda x, y: x + y)
print("---------- PartA Q2 Answer ----------")
for department, sum in salary_sum.collect():
    print(department + ": " + str(sum))
print("-------------------------------------")

########## PartA Q3 ##########
shakespeare_rdd = sc.textFile("/Users/liulei/Desktop/mie1628/ass2/shakespeare.txt")
word_list = ['Shakespeare', 'When', 'Lord', 'Library', 'GUTENBERG', 'WILLIAM',
             'COLLEGE', 'WORLD']
word_counts = shakespeare_rdd.flatMap(lambda line: line.split(" ")).filter(lambda word: word in word_list).map(lambda
    word: (word, 1)).reduceByKey(lambda x, y: x + y)
print("---------- PartA Q3 Answer ----------")
for word, counts in word_counts.collect():
    print(word + ": " + str(counts))
print("-------------------------------------")

########## PartA Q4 ##########
counts = shakespeare_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(
    lambda x, y: x + y)
ascending = counts.sortBy(lambda x: x[1]).take(15)
descending = counts.sortBy(lambda x: x[1], False).take(15)
print("---------- PartA Q4 Answer ----------")
print("---------- Top 15 words used----------")
for word, counts in descending:
    print(word + ": " + str(counts))
print("---------- Bottom 15 words used----------")
for word, counts in ascending:
    print(word + ": " + str(counts))
print("-------------------------------------")

########## PartB ##########
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit
########## PartB Q1 ##########
df = spark.read.option("header",True).csv("/Users/liulei/Desktop/mie1628/ass2/movies.csv")
print("---------- PartB Q1 Answer ----------")
print(df.describe())
print("---------- Top 12 movies with the highest ratings ----------")
df.groupBy("movieId").agg(avg("rating").alias("avg_rating")).sort(col("avg_rating").desc()).limit(12).show()
print("---------- Top 12 users provide highest ratings ----------")
df.groupBy("userId").agg(avg("rating").alias("avg_rating")).sort(col("avg_rating").desc()).limit(12).show()
print("-------------------------------------")

########## PartB Q2 ##########
print("---------- PartB Q2 Answer ----------")
df = (
    spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .option("sep", ",")
    .option("path", "/Users/liulei/Desktop/mie1628/ass2/movies.csv")
    .load()
)
#split the data 80/20
(training_80, test_20) = df.randomSplit([0.8, 0.2])
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")
model_8_2 = als.fit(training_80)
predictions_8_2 = model_8_2.transform(test_20)
# Evaluate the model by computing the RMSE on the test data
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse_8_2 = evaluator.evaluate(predictions_8_2)
print("80/20 Root-mean-square error = " + str(rmse_8_2))

#split the data 60/40
(training_60, test_40) = df.randomSplit([0.6, 0.4])
model_6_4 = als.fit(training_60)
predictions_6_4 = model_6_4.transform(test_40)
rmse_6_4 = evaluator.evaluate(predictions_6_4)
print("60/40 Root-mean-square error = " + str(rmse_6_4))
print("-------------------------------------")

########## PartB Q3 ##########
print("---------- PartB Q3 Answer ----------")
metrics_list = ["rmse", "mse", "mae"]
for metrics in metrics_list:
    evaluator = RegressionEvaluator(metricName=metrics, labelCol="rating",
                                predictionCol="prediction")
    result_8_2 = evaluator.evaluate(predictions_8_2)
    result_6_4 = evaluator.evaluate(predictions_6_4)
    print("80/20 " + metrics + " = " + str(result_8_2))
    print("60/40 " + metrics + " = " + str(result_6_4))
print("-------------------------------------")

########## PartB Q4 ##########
print("---------- PartB Q4 Answer ----------")
#Tune model using ParamGridBuilder
parameters = (ParamGridBuilder()
             .addGrid(als.rank, [5, 10, 15])
             .addGrid(als.maxIter, [10, 15])
             .addGrid(als.regParam, [0.01, 0.1, 0.5, 1.0])
             .build())
cv = CrossValidator(estimator=als,
                    estimatorParamMaps=parameters,
                    evaluator = evaluator,
                    numFolds=3)
cvmodel = cv.fit(training_80)

print(f"Best Rank: {cvmodel.bestModel.rank}")
print(f"Best MaxIter: {cvmodel.bestModel._java_obj.parent().getMaxIter()}")
print(f"Best RegParam: {cvmodel.bestModel._java_obj.parent().getRegParam()}")

cvpredictions = cvmodel.bestModel.transform(test_20)
cv_rmse = evaluator.evaluate(cvpredictions)
print("Best Output Root-mean-square error = : "+ str(cv_rmse))
print("-------------------------------------")

########## PartB Q5 ##########
print("---------- PartB Q5 Answer ----------")
user_subset = spark.createDataFrame([Row(userId=10), Row(userId=12)])
user_subset_recs = cvmodel.bestModel.recommendForUserSubset(user_subset, 12)
user_subset_recs.show(truncate=False)
print("-------------------------------------")