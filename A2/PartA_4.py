# Databricks notebook source
import string

feelistRDD = spark.sparkContext.textFile("/FileStore/tables/shakespeare_1.txt")

wordSplit = feelistRDD.map(lambda line: line.translate(str.maketrans('', '', string.punctuation)))

words = wordSplit.flatMap(lambda line: line.split(" ")).filter(lambda x: x != '')
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda x,y : x+y)
reverseRDD = wordCounts.map(lambda word: (word[1], word[0]))
reverseRDD.collect()
topMostWordCountsRDD = reverseRDD.sortByKey(False).map(lambda word: (word[1], word[0]))
topLeastWordCountsRDD = reverseRDD.sortByKey(True).map(lambda word: (word[1], word[0]))

print("Top 10 words with the most count: \n", topMostWordCountsRDD.take(10))
print("Top 10 words with the least count: \n", topLeastWordCountsRDD.take(10))

