sbt assembly
~/spark/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class main.Application --master spark://ip-172-31-57-38:7077 --conf "spark.executor.memory=2g" ~/tellius_cassandra_spark_test/target/scala-2.10/tellius_cassandra_spark_test-assembly-0.0.1.jar
