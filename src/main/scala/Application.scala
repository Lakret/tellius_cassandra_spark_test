package main

import org.apache.spark._
import com.datastax.spark.connector._

// ~/home/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class main.Middleware --master spark://ip-172-31-57-38:7077 tellius_cassandra_spark_test-assembly-0.0.1.jar

object Application extends App  {
// bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.10 --master spark://ip-172-31-57-38:7077 --driver-java-options spark.driver.allowMultipleContexts=true

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.57.38").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext("spark://ip-172-31-57-38:7077", "text", conf)
  val airlines = sc.cassandraTable("testairlines", "airlines")

  println("running...")

  println(airlines.select("year", "month", "day", "uniquecarrier", "arrdelay").where("year = ? and month = ? and uniquecarrier = ?", "2007", "1", "WN").map(_.getInt("arrdelay")).sum)
}
