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
Run analytics on dta stored in Delta Lake.

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


## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch17

2. cd net.jgp.books.spark.ch17

3. Package application using sbt command

   ```
     sbt clean assembly
   ```

4. Run Spark/Scala application using spark-submit command as shown below:

   ```
   spark-submit --class net.jgp.books.spark.ch17.lab100_export.ExportWildfiresScalaApplication target/scala-2.12/SparkInAction2-Chapter17-assembly-1.0.0.jar  
   ```






spark-submit --driver-class-path ~/.ivy2/jars/io.delta_delta-core_2.12-0.7.0.jar  --packages io.delta:delta-core_2.12:0.7.0 feedDeltaLakeApp.py

spark-submit --driver-class-path ~/.ivy2/jars/io.delta_delta-core_2.12-0.7.0.jar  --packages io.delta:delta-core_2.12:0.7.0 meetingsPerDepartmentApp.py

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkWithJava/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).

[1]: https://data.cityofnewyork.us
