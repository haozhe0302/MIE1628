# Databricks notebook source
raw_data  = spark.sparkContext.textFile("/FileStore/tables/movies.csv")
raw_data_header = raw_data.take(1)[0]
print("Raw data header: ", raw_data_header)
print("--------------------------------")

data = raw_data.filter(lambda line: line!=raw_data_header).map(lambda line: line.split(",")).map(lambda tokens:(tokens[2],tokens[0],tokens[1])).cache()

movieRDD = data.map(lambda word: (word[1], int(word[2])))  
movieCounts = movieRDD.reduceByKey(lambda a,b:a +b)
reverseMovie = movieCounts.map(lambda word: (word[1], word[0]))
reverseMovie.collect()
topRatingMovie = reverseMovie.sortByKey(False).map(lambda word: (word[1], word[0]))
print("Top 20 movies with the highest ratings: \n Format: (movie id, 'rating score')")
print(topRatingMovie.take(20))
print("--------------------------------")

userRDD = data.map(lambda word: (word[0], int(word[2])))  
userCounts = userRDD.reduceByKey(lambda a,b:a +b)
reverseUser = userCounts.map(lambda word: (word[1], word[0]))
reverseUser.collect()
topRatingUser = reverseUser.sortByKey(False).map(lambda word: (word[1], word[0]))
print("Top 15 users who provided the highest ratings: \n Format: (user id, 'rating num')")
print(topRatingUser.take(15))
print("--------------------------------")

