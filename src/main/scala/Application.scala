package main

import java.io.{FileReader, BufferedReader, File}
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors

import org.apache.spark._
import com.datastax.spark.connector._

import com.datastax.driver.core._

import scala.collection.immutable._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.forkjoin._


import scala.io.Source

// ~/home/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class main.Middleware --master spark://ip-172-31-57-38:7077 tellius_cassandra_spark_test-assembly-0.0.1.jar

object CassandraTestLocal {
 implicit val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))

  def insertData(table: String) = {
    val cluster = Cluster.builder().addContactPoint("172.31.58.106").build()
    val session = cluster.connect("testairlines")

    println("connected")

    val lines = Source.fromFile("./data/2007.csv").getLines()

    println("lines read")
    println(java.time.LocalDateTime.now())

    var x = 0
    lines.foreach { row =>
     Future {
      var arr: Array[String] = row.split(",").array
      for (i <- 0 to 13) {
       arr(i) = "'" +  arr(i) + "'"
      }
      for (i <- 15 to (arr.length - 1)) {
       arr(i) = "'" +  arr(i) + "'"
      }

      val prepared = arr.mkString(",")
      x = x + 1
      if (x % 10000 == 0) {
        println(x, prepared)
      }

//        println("executing")
        val stmnt = "INSERT INTO airlines (year,month,day ,dayofweek , deptime , crsdeptime , arrtime , crsarrtime , uniquecarrier , flightnum , tailnum , actualelapsedtime , crselapsedtime , airtime , arrdelay, depdelay, origin , dest , distance , taxiin , taxiout , cancelled , cancellationcode , diverted , carrierdelay , weatherdelay , nasdelay , securitydelay , lateaircraftdelay ) VALUES (" + prepared + ");"
//        println(stmnt)
        val res =  session.executeAsync(stmnt)
      if (x % 10000 == 0) {
        println(res.get(1, TimeUnit.SECONDS))
      }
      }
    }

    println(java.time.LocalDateTime.now())
  }

  def queryData(table: String) = {

    val cluster = Cluster.builder().addContactPoint("172.31.58.106").build()
    val session = cluster.connect("testairlines")

    println("sum(arrdelay) for 2007 and WN")
    println(java.time.LocalDateTime.now())
    lazy val futures = (1 to 12).map { month =>
     Future {
      val stmnt = "SELECT sum(arrdelay) from airlines where year = '2007' and month = '" + month.toString + "' and uniquecarrier = 'WN'"
      var res = session.execute(stmnt)
      println(res)
     }
    }
    Await.ready(Future.sequence(futures), Duration.Inf)
    println(java.time.LocalDateTime.now())

   println("sum for each carrier")
   println(java.time.LocalDateTime.now())
   val carriers = List("WN", "UA", "OO", "NW", "MQ", "HA", "AA", "US", "AQ", "XE", "OH", "DL", "B6", "9E", "AS", "CO", "F9", "YV", "EV", "FL")
   lazy val futures3 = (1 to 12).flatMap { month =>
    carriers.map { carrier =>
     Future {
      val stmnt = "SELECT sum(arrdelay) from airlines where year = '2007' and month = '" + month.toString + "' and uniquecarrier = '" + carrier + "'"
      var res = session.execute(stmnt)
     }
    }
   }
   Await.ready(Future.sequence(futures3), Duration.Inf)
   println(java.time.LocalDateTime.now())

   println("top 100 for 2007 and WN")
   println(java.time.LocalDateTime.now())
   lazy val futures2 = (1 to 12).map { month =>
    Future {
     val stmnt = "SELECT arrdelay from airlines where year = '2007' and month = '" + month.toString + "' and uniquecarrier = 'WN' limit 1000"
     var res = session.execute(stmnt)
    }
   }
   Await.ready(Future.sequence(futures2), Duration.Inf)
   println(java.time.LocalDateTime.now())
  }
}

object Application extends App  {
// bin/spark-shell --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.10 --master spark://ip-172-31-57-38:7077 --driver-java-options spark.driver.allowMultipleContexts=true
 def sparkTest() = {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.58.106").set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext("spark://ip-172-31-58-106:7077", "text", conf)
  val airlines = sc.cassandraTable("testairlines", "airlines")

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

  println("taking top 100 arrdelays for WN in 2007")
  println(airlines
    .select("year", "uniquecarrier", "arrdelay")
    .filter(row => row.getInt("year") == 2007 && row.getString("uniquecarrier") == "WN")
    .map(row => (row.getFloat("arrdelay"), (row.getInt("year"), row.getString("uniquecarrier"))))
    .sortByKey(false)
    .take(100))

  //  println("selecting top 100 delays for WN")
  //  val top100ByCarrier = airlines
  //    .select("year", "uniquecarrier", "arrdelay")
  //    .where("year = ?", "2007")
  //    .spanBy(row => row.getString("uniquecarrier"))
  //    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
  //    .toString()
  //  println(top100ByCarrier)

//  println("starting caching...")
//  airlines.cache()
//  println("cached.")
//
//  println("cached: cassandra count:")
//  println(airlines.cassandraCount())
//
//  println("cached: spark count:")
//  println(airlines.count())
//
//  println("cached: selecting sum(arrdelay) for WN in January of 2007")
//  println(airlines.select("year", "month", "day", "uniquecarrier", "arrdelay").where("year = ? and month = ? and uniquecarrier = ?", "2007", "1", "WN").map(_.getInt("arrdelay")).sum)
//
//  println("cached: selecting sum(arrdelay) for WN in 2007")
//  println(airlines
//    .select("year", "uniquecarrier", "arrdelay")
//    .filter(row => row.getInt("year") == 2007 && row.getString("uniquecarrier") == "WN")
//    .map(_.getInt("arrdelay")).sum)
//
//  println("cached: selecting avg(arrdelay) for each carrier in 2007")
//  val arrdelayByCarrier2 = airlines
//    .select("year", "uniquecarrier", "arrdelay")
//    .filter(row => row.getInt("year") == 2007)
//    .spanBy(row => row.getString("uniquecarrier"))
//    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
//    .collect()
//  arrdelayByCarrier2.foreach{ case (carrier, delay) => println(carrier + " - " + delay) }
//
//  println("now use custom in-memory caching for the last query: ")
//  val arrdelayByCarrierCached = airlines
//    .select("year", "uniquecarrier", "arrdelay")
//    .filter(row => row.getInt("year") == 2007)
//    .spanBy(row => row.getString("uniquecarrier"))
//    .cache()
//
//  println(arrdelayByCarrierCached.count())
//  println("cached")
//
//  val arrdelayByCarrier3 = arrdelayByCarrierCached
//    .map {case (carrier, rows) => (carrier, rows.map(_.getInt("arrdelay")).sum)}
//    .collect()
//
//  arrdelayByCarrier3.foreach{ case (carrier, delay) => println(carrier + " - " + delay) }
//
//  println("cached: taking top 100 arrdelays for WN in 2007")
//  println(airlines
//    .select("year", "uniquecarrier", "arrdelay")
//    .filter(row => row.getInt("year") == 2007 && row.getString("uniquecarrier") == "WN")
//    .map(row => (row.getFloat("arrdelay"), (row.getInt("year"), row.getString("uniquecarrier"))))
//    .sortByKey(false)
//    .take(100))
  }


  println("hi!")
//  CassandraTestLocal.insertData("airlines")
//  sparkTest()
  CassandraTestLocal.queryData("airlines")
}
