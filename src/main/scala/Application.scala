package main

import org.apache.spark._
import com.datastax.spark.connector._

object Middleware extends App  {
// bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.10 --master spark://ip-172-31-57-38:7077 --driver-java-options spark.driver.allowMultipleContexts=true

  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.57.38").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext("spark://ip-172-31-57-38:7077", "text", conf)
  val airlines = sc.cassandraTable("testairlines", "airlines")

  println("running...")

  println(airlines.first)
}
