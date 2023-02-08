# Databricks notebook source
feelistRD = spark.sparkContext.textFile("/FileStore/tables/integer.txt")
feelistRD.collect()

arrayRDD = feelistRD.map(lambda x: int(x)%2)
arrayRDD.collect()

reduce = arrayRDD.map(lambda word: (word, 1))
reduce.collect()

res = reduce.reduceByKey(lambda x,y : x + y) 

print("Even Number: ", res.collect()[0][1])
print("Odd Number: ", res.collect()[1][1])
