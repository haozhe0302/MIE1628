# Databricks notebook source
import math
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.evaluation import RegressionMetrics

raw_data  = spark.sparkContext.textFile("/FileStore/tables/movies.csv")
raw_data_header = raw_data.take(1)[0]
data = raw_data.filter(lambda line: line!=raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (tokens[2],tokens[0],tokens[1])).cache()

print("80/20 Train & Test Split")
training_RDD, test_RDD = data.randomSplit([0.8, 0.2], seed=12)
test_for_predict_RDD = test_RDD.map(lambda line : (line[0], line[1]))

model = ALS.train(training_RDD, 4, seed=12, iterations=10, lambda_=0.1)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r : ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r : ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)

values_and_preds = rates_and_preds.map(lambda line : (line[1][0], line[1][1]))
metrics = RegressionMetrics(values_and_preds)

print("MSE: %s" % metrics.meanSquaredError)
print("RMSE: %s" % metrics.rootMeanSquaredError)
print("MAE: %s" % metrics.meanAbsoluteError)
print("--------------------------------")


print("60/40 Train & Test Split")
training_RDD, test_RDD = data.randomSplit([0.6, 0.4], seed=12)
test_for_predict_RDD = test_RDD.map(lambda x : (x[0], x[1]))

model = ALS.train(training_RDD, 4, seed=12, iterations=10, lambda_=0.1)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r : ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r : ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)

values_and_preds = rates_and_preds.map(lambda line : (line[1][0], line[1][1]))
metrics = RegressionMetrics(values_and_preds)

print("MSE: %s" % metrics.meanSquaredError)
print("RMSE: %s" % metrics.rootMeanSquaredError)
print("MAE: %s" % metrics.meanAbsoluteError) 
print("--------------------------------")
                                                                                                 
