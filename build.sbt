import sbt._

name := "tellius_cassandra_spark_test"

version := "0.0.1"

scalaVersion := "2.10.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

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
     // "org.apache.spark"  %% "spark-hive"      % sparkVersion,
     // "org.apache.spark"  %% "spark-repl"      % sparkVersion,
     "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"
  )
}

assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last

    case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
    case PathList("io", "netty", xs @ _*) => MergeStrategy.last

    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case PathList("org", "datanucleus", xs @ _*) => MergeStrategy.last

    case "parquet.thrift" => MergeStrategy.last
    
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}

