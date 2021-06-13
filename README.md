This repository contains the Java labs as well as their Scala and Python ports of the code used in Manning Publication’s **[Spark in Action, 2nd edition](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp)**, by Jean-Georges Perrin.

---

# Spark in Action, 2nd edition – Java, Python, and Scala code for chapter 17

Chapter 17 is about exporting data.

This code is designed to work with:
 * Apache Spark v3.0.0.
 * Delta Lake v0.7.0.

## Labs

### Lab \#100
Adapted from one of IBM's [Code for Call starter kit](https://developer.ibm.com/callforcode/starters/water), but leveraging the [wildfires category](https://developer.ibm.com/code-and-response/disasters/wildfires).

### Lab \#200
Feeds data into Delta Lake.

### Lab \#210, \#220
Run analytics on data stored in Delta Lake.

### Lab \#250
Feeds data into Delta Lake.

### Lab \#900
Append using primary key.

### Lab \#910
Export widlfire data to Elasticsearch.

## Datasets

Dataset(s) used in this chapter:

 * https://www.data.gouv.fr/en/datasets/donnees-ouvertes-du-grand-debat-national/#_
 * Populations légales 2016 https://www.insee.fr/fr/statistiques/3677785?sommaire=3677855

### Lab \#100

The `ExportWildfiresApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	Spark stores the contents in a dataframe, do some transformations and export final reports as CSV data.

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch17
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch17/src/main/python/lab100_orders/
```

3. Execute the following spark-submit command to create a jar file to our this application

 ```
spark-submit orderStatisticsApp.py
 ```

NOTE:- 
If you want to run delta lake examples, please use the following spark-submit command:

```
spark-submit --driver-class-path /tmp/jars/io.delta_delta-core_2.12-0.7.0.jar  --packages io.delta:delta-core_2.12:0.7.0 feedDeltaLakeApp.py
```

If you want to run PostgreSQL  example, please use the following spark-submit command:

```
spark-submit --driver-class-path /tmp/jars/org.postgresql_postgresql-42.1.4.jar  --packages org.postgresql:postgresql:42.1.4 appendDataJdbcPrimaryKeyApp.py
```

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

1. Clone this project
```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch17
```
2. cd net.jgp.books.spark.ch17

3. Package application using sbt command

```
 sbt clean assembly
```

4. Run Spark/Scala application using spark-submit command as shown below:

```
spark-submit --class net.jgp.books.spark.ch17.lab100_export.ExportWildfiresScalaApplication target/scala-2.12/SparkInAction2-Chapter17-assembly-1.0.0.jar  
```

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).

[1]: https://data.cityofnewyork.us
