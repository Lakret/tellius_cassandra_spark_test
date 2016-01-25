package main

import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

// ~/home/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class main.Middleware --master spark://ip-172-31-57-38:7077 tellius_cassandra_spark_test-assembly-0.0.1.jar

object Application extends App  {
// bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.10 --master spark://ip-172-31-57-38:7077 --driver-java-options spark.driver.allowMultipleContexts=true

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.57.38").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext("spark://ip-172-31-57-38:7077", "text", conf)
  val airlines = sc.cassandraTable("testairlines", "airlines5")

  println("running...")

  println("cassandra count:")
  println(airlines.cassandraCount())

  println("spark count:")
  println(airlines.count())

  println("selecting sum(arrdelay) for WN in January of 2007")
  println(airlines.select("year", "month", "day", "uniquecarrier", "arrdelay").where("year = ? and month = ? and uniquecarrier = ?", "2007", "1", "WN").map(_.getInt("arrdelay")).sum)

  println("selecting sum(arrdelay) for WN in 2007")
//  fails because not all keys of partition key are provided
//  TODO: retest with different partition key
//  println(airlines.select("year", "uniquecarrier", "arrdelay").where("year = ? and uniquecarrier = ?", "2007", "WN").map(_.getInt("arrdelay")).sum)
  println(airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007 && row.getString("uniquecarrier") == "WN")
    .map(_.getInt("arrdelay")).sum)

  println("selecting avg(arrdelay) for each carrier in 2007")
//  TODO: try where with different partitioning
  val arrdelayByCarrier = airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007)
    .spanBy(row => row.getString("uniquecarrier"))
    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
    .collect()
  arrdelayByCarrier.foreach{ case (carrier, delay) => println(carrier + " - " + delay) }


//  TODO:
//  top100, joins, averages

//  println("selecting top 100 delays for WN")
//  val top100ByCarrier = airlines
//    .select("year", "uniquecarrier", "arrdelay")
//    .where("year = ?", "2007")
//    .spanBy(row => row.getString("uniquecarrier"))
//    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
//    .toString()
//  println(top100ByCarrier)

  println("starting caching...")
  airlines.cache()
  println("cached.")

  println("cached: cassandra count:")
  println(airlines.cassandraCount())

  println("cached: spark count:")
  println(airlines.count())

  println("cached: selecting sum(arrdelay) for WN in January of 2007")
  println(airlines.select("year", "month", "day", "uniquecarrier", "arrdelay").where("year = ? and month = ? and uniquecarrier = ?", "2007", "1", "WN").map(_.getInt("arrdelay")).sum)

  println("cached: selecting sum(arrdelay) for WN in 2007")
  println(airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007 && row.getString("uniquecarrier") == "WN")
    .map(_.getInt("arrdelay")).sum)

  println("cached: selecting avg(arrdelay) for each carrier in 2007")
  val arrdelayByCarrier2 = airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007)
    .spanBy(row => row.getString("uniquecarrier"))
    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
    .collect()
  arrdelayByCarrier2.foreach{ case (carrier, delay) => println(carrier + " - " + delay) }

  println("now use custom in-memory caching for the last query: ")
  val arrdelayByCarrierCached = airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007)
    .spanBy(row => row.getString("uniquecarrier"))
    .cache()
  println("cached")

  val arrdelayByCarrier3 = arrdelayByCarrierCached
    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
    .collect()

  arrdelayByCarrier3.foreach{ case (carrier, delay) => println(carrier + " - " + delay) }
}
