import sbt._

name := "tellius_cassandra_spark_test"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers += "spray repo" at "http://repo.spray.io"

resolvers += "spray nightlies" at "http://nightlies.spray.io"

libraryDependencies ++= {
  val akkaVersion = "2.3.6"
  val sprayVersion = "1.3.1"
  val sparkVersion = "1.5.2"
  Seq(
    // "com.datastax.cassandra"  % "cassandra-driver-core" % "2.1.1"  exclude("org.xerial.snappy", "snappy-java"),
     "org.apache.spark"  %% "spark-core"      % sparkVersion,
     "org.apache.spark"  %% "spark-streaming" % sparkVersion,
     "org.apache.spark"  %% "spark-sql"       % sparkVersion,
     "org.apache.spark"  %% "spark-hive"      % sparkVersion,
     "org.apache.spark"  %% "spark-repl"      % sparkVersion,
     "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"
  )
}
