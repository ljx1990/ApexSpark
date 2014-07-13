package org.apex.spark.ta

import org.apache.spark._
import org.apache.spark.SparkContext._
class WordCount {

}

object WordCount {
  def main(args: Array[String]) {
    val spark =  new SparkContext(args(0), "WordCount",
      System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    val file = spark.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/spart_test.txt")
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://localhost:8020/user/lvjx/data/lvjx/spart_test_20140703.txt")
    spark.stop()
  }
}