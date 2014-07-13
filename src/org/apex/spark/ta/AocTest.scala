package org.apex.spark.ta

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
class AocTest {

}

/**
 * 统计词频
 * 
 * 
 * 注意:该词频存在问题,原因待查
 * 
 * hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt  投诉内容描述
 * hdfs://localhost:8020/user/lvjx/data/lvjx/dict.txt  投诉关键字
 */
object AocTest {
  def main(args: Array[String]) {
    val sc =  new SparkContext(args(0), "WordCount",
      System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
   
    val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my.txt")
    val dict_file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my_dict.txt");
   
    /**
     * 必须先执行collect求出结果
     */
    val dd = dict_file.flatMap(line1 => line1.split("\n")).collect;
    val counts = file.flatMap{
    	line =>
    	  var tmp =ArrayBuffer[String]();
    	  for (elem<-dd){
    	    if(line.contains(elem)){
    	      tmp +=elem
    	    }
    	  }
    	  tmp
    	}.map(word => (word, 1)).reduceByKey(_ + _).map(x =>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
	counts.collect()
    sc.stop()
  }
}