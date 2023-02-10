# Databricks notebook source
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder, TrainValidationSplitModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

paramGrid = ParamGridBuilder().addGrid(ALS.rank, [5, 10, 15]).addGrid(ALS.maxIter, [5, 10, 15]).addGrid(ALS.regParam, [0.01, 0.1, 0.5, 2]).build()

print("80/20 Train & Test Split")
print("Tune the parameters to get the best set")

path = "/FileStore/tables/movies.csv"
df = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").option("path", path).load()

trainSet, testSet = df.randomSplit([0.8, 0.2], seed=12)

als = ALS(
         userCol="userId", 
         itemCol="movieId",
         ratingCol="rating", 
         nonnegative = True, 
         implicitPrefs = False,
         coldStartStrategy="drop"
)

evaluator = lr_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="mae")
tvs = TrainValidationSplit(estimator=als, estimatorParamMaps=paramGrid, evaluator=evaluator, parallelism=1, seed=12)

model = tvs.fit(trainSet)
predictions = model.bestModel.transform(testSet)
error = evaluator.evaluate(predictions)

print("--------------------------------")
print("Best Paramter Set: ")
print("Best Rank: " , model.bestModel._java_obj.parent().getRank())
print("Max Iteration: " , model.bestModel._java_obj.parent().getMaxIter())
print("Best Regularization Parameter: " , model.bestModel._java_obj.parent().getRegParam())
print("MAE of test set: ", error) 

