# Databricks notebook source
feelistRDD = spark.sparkContext.textFile("/FileStore/tables/shakespeare_1.txt")

words = feelistRDD.flatMap(lambda line: line.split(" "))

wordCounts = words.map(lambda word : (word, 1)).reduceByKey(lambda x,y : x+y)

res = []
for i in wordCounts.collect():
    if i[0] in ["Shakespeare", "why", "Lord", "Library", "GUTENBERG", "WILLIAM", "COLLEGE", "WORLD"]:
        res.append(i)
        
print(res)

