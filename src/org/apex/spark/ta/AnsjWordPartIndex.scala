package org.apex.spark.ta

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.ansj.splitWord.analysis.ToAnalysis
class AnsjWordPartIndex {

}

/**
* 基于Ansj的统计词频,
* 索引的方式
 * 
 * hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt 
 * hdfs://localhost:8020/user/lvjx/data/lvjx/dict.txt  
 */
object AnsjWordPartIndex {
  def main(args: Array[String]) {
    val sc =  new SparkContext(args(0), "WordCount",
    System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
    //val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt")
    val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my.txt")
   // sc.addJar("/software/CnWordPart.jar");
  
	val counts = file.flatMap{
	  line =>  ToAnalysis.simpleParse(line).toArray()
	}
	.map(word => (word, 1)).reduceByKey(_ + _).map(x =>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
	counts.collect()
	counts.saveAsTextFile("hdfs://localhost:8020/user/lvjx/data/lvjx/ansjword")
    sc.stop()
  }
}


             








