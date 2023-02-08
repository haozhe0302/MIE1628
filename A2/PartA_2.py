# Databricks notebook source
feelistRD = spark.sparkContext.textFile("/FileStore/tables/salary.txt")
feelistRD.collect()

arrayRDD = feelistRD.map(lambda x: x.split(" "))
arrayRDD.collect()

toNumber = arrayRDD.map(lambda x : (x[0], int(x[1]))) 
toNumber.collect()
sumRDD = toNumber.reduceByKey(lambda x,y : x + y) 
print(sumRDD.collect())

