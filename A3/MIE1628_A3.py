# Databricks notebook source
# MAGIC %md
# MAGIC #PartA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.
# MAGIC Use the python urllib library to extract the KDD Cup 99 data from their web repository, store it in a temporary location and then move it to the Databricks filesystem which can enable easy access to this data for analysis. Use the following commands in Databricks to get your data.

# COMMAND ----------

import urllib.request
urllib.request.urlretrieve("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "/tmp/kddcup_data.gz")
dbutils.fs.mv("file:/tmp/kddcup_data.gz", "dbfs:/kdd/kddcup_data.gz")
display(dbutils.fs.ls("dbfs:/kdd"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.
# MAGIC After storing the data in the Databricks filesystem. Load your data from the disk into Spark's RDD. Print 10 values of your RDD and verify the type of data structure of your data (RDD).

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

raw_data  = spark.sparkContext.textFile("dbfs:/kdd/kddcup_data.gz")
print("10 values of RDD: \n", raw_data.take(10))
print("Type of data structure of the dataset: \n", type(raw_data.take(10)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.
# MAGIC Split the data. (Each entry in your RDD is a comma-separated line of data, which you first need to split before you can parse and build your data frame.) Show the total number of features (columns) and print results.

# COMMAND ----------

print(raw_data.take(1)[0])
print("Total num of features: ", len(raw_data.take(1)[0].split(',')))

data = raw_data.map(lambda line: line.split(",")).cache()
df = data.toDF()
df.printSchema()
df.show(truncate=False)
print(df.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.
# MAGIC Now extract these 6 columns (duration, protocol_type, service, src_bytes, dst_bytes, flag and label) from your dataset. Build a new RDD and data frame. Print schema and display 10 values.

# COMMAND ----------

data_selected = data.map(lambda line: (line[0],line[1],line[2],line[3],line[4],line[5])).cache()
print(data_selected.take(2))

deptColumns = ["duration","protocol_type", "service", "src_bytes", "dst_bytes", "flag and label"]
df_selected = data_selected.toDF(deptColumns)
df_selected.printSchema()
print(df_selected.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.
# MAGIC Get the total number of connections based on the protocol_type and based on the service. Show the result in ascending order. Plot the bar graph for both.

# COMMAND ----------

intRDD = data.map(lambda line: (line[1], 1))
intCountsPre1 = intRDD.reduceByKey(lambda a,b : a+b)
intCountsPre2 = intCountsPre1.map(lambda word: (word[1], word[0]))
intCounts = intCountsPre2.sortByKey(True)
barRDD = intCounts.map(lambda word: (word[1], word[0]))

df_selected_protocol_type = barRDD.toDF(["protocol_type", "count"])
pd_selected_protocol_type = df_selected_protocol_type.toPandas()
pd_selected_protocol_type.sort_values(by=['count'], inplace=True)
display(pd_selected_protocol_type)
pd_selected_protocol_type.set_index('protocol_type', inplace=True)
pd_selected_protocol_type.plot(kind="bar")


intRDD = data.map(lambda line: (line[2], 1))
intCountsPre1 = intRDD.reduceByKey(lambda a,b : a+b)
intCountsPre2 = intCountsPre1.map(lambda word: (word[1], word[0]))
intCounts = intCountsPre2.sortByKey(True)
barRDD = intCounts.map(lambda word: (word[1], word[0]))

df_selected_service = barRDD.toDF(["service", "count"])
pd_selected_service = df_selected_service.toPandas()
pd_selected_service.sort_values(by=['count'], inplace=True)
display(pd_selected_service)
pd_selected_service.set_index('service', inplace=True)
pd_selected_service.plot(kind="bar", figsize=(20,5))

# COMMAND ----------

df_selected_protocol_type = df_selected.groupBy("protocol_type").count()
df_selected_protocol_type = df_selected_protocol_type.sort("count")
display(df_selected_protocol_type)

# COMMAND ----------

df_selected_service = df_selected.groupBy("service").count()
df_selected_service = df_selected_service.sort("count")
display(df_selected_service)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.
# MAGIC Do a further exploratory data analysis, including other columns of this dataset and plot graphs. Plot at least 3 different charts/plots and explain them.

# COMMAND ----------

intRDD = data.map(lambda line: (line[3], 1))
intCountsPre1 = intRDD.reduceByKey(lambda a,b : a+b)
intCountsPre2 = intCountsPre1.map(lambda word: (word[1], word[0]))
intCounts = intCountsPre2.sortByKey(True)
barRDD = intCounts.map(lambda word: (word[1], word[0]))

df_flag = barRDD.toDF(["flag", "count"])
pd = df_flag.toPandas()
pd.set_index('flag', inplace=True)
pd.plot(kind="bar")
display(df_flag)

# COMMAND ----------

intRDD = data.map(lambda line: (line[41], 1))
intCountsPre1 = intRDD.reduceByKey(lambda a,b : a+b)
intCountsPre2 = intCountsPre1.map(lambda word: (word[1], word[0]))
intCounts = intCountsPre2.sortByKey(True)
barRDD = intCounts.map(lambda word: (word[1], word[0]))

df_label = barRDD.toDF(["label", "count"])
pd = df_label.toPandas().head(20)
pd.set_index('label', inplace=True)
pd.plot(kind="bar", figsize=(20,5))
display(df_label)

# COMMAND ----------

intRDD = data.map(lambda line: (line[28], 1))
intCountsPre1 = intRDD.reduceByKey(lambda a,b : a+b)
intCountsPre2 = intCountsPre1.map(lambda word: (word[1], word[0]))
intCounts = intCountsPre2.sortByKey(True)
barRDD = intCounts.map(lambda word: (word[1], word[0]))

df_same_srv_rate = barRDD.toDF(["same_srv_rate", "count"])
pd = df_same_srv_rate.toPandas().head(50)
pd.set_index('same_srv_rate', inplace=True)
pd.plot(kind="bar", figsize=(20,5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.
# MAGIC Look at the label column where label == ‘normal’. Now create a new label column where you have a label == ‘normal’ and everything else is considered as an ‘attack’. Split your data (train/test) and based on your new label column now build a simple machine learning model for intrusion detection (you can use a few selected columns for your model out of all). Apply 2 different algorithms.

# COMMAND ----------

dataset = data.toDF().toPandas()
dataset.insert(loc=42, column='class', value="normal")
dataset.loc[dataset["_42"]!='normal.','class'] = "attack"
print(dataset)

# COMMAND ----------

from sklearn import preprocessing
from sklearn.model_selection import train_test_split

le = preprocessing.LabelEncoder()

le.fit(dataset.iloc[:,1:2])
dataset.iloc[:,1:2] = le.fit_transform(dataset.iloc[:,1:2])
le.fit(dataset.iloc[:,2:3])
dataset.iloc[:,2:3] = le.fit_transform(dataset.iloc[:,2:3])
le.fit(dataset.iloc[:,3:4])
dataset.iloc[:,3:4] = le.fit_transform(dataset.iloc[:,3:4])
le.fit(dataset.iloc[:,42])
dataset.iloc[:,42] = le.fit_transform(dataset.iloc[:,42])

y = dataset.iloc[:,42]
X = dataset.iloc[:,0:6]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Linear SVC

# COMMAND ----------

from sklearn import svm

lsvc = svm.LinearSVC(dual=False)
lsvc.fit(X_train, y_train)
y_pred_lsvc = lsvc.predict(X_test.iloc[:,:41])
print("Linear SVC Train Accruacy: ", lsvc.score(X_train, y_train))
print("Linear SVC Test Accruacy: ", lsvc.score(X_test, y_test))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 KNN

# COMMAND ----------

from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score

knn = KNeighborsClassifier(n_neighbors = 5)
knn.fit(X_train, y_train)
y_pred_train_knn = knn.predict(X_train)
y_pred_test_knn = knn.predict(X_test)
print("KNN Train Accuracy: ", accuracy_score(y_train, y_pred_train_knn))
print("KNN Test Accuracy: ", accuracy_score(y_test, y_pred_test_knn))
