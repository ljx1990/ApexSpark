package org.apex.spark.ta

import org.apache.spark._
import org.apache.spark.SparkContext._
//import ICTCLAS.I3S.AC.ICTCLAS50
//import ICTCLAS.I3S.UTILS.CnWordPartUtil
//import ICTCLAS.I3S.UTILS.CnWordConstant
class AocWordPart {

}

/**
 * 用户挖掘套餐中词频的MR
 * 
 * 
 * 宣告失败!!!!C调度占用内存过多导致溢出,由于没有源代码所以无法
 * 
 * hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt  投诉内容描述
 * hdfs://localhost:8020/user/lvjx/data/lvjx/dict.txt  投诉关键字
 */
object AocWordPart {
  def main(args: Array[String]) {
    val sc =  new SparkContext(args(0), "WordCount",
    System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass))
   
    //val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt")
    val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my.txt")
   // sc.addJar("/software/CnWordPart.jar");
    /**
     * 必须要加collect,以执行MR分析操作
     */
   /* val counts = file.flatMap{
      line => CnWordPartUtil.testICTCLAS_ParagraphProcess_User(line, CnWordConstant.PKULEVEL2).split(" ")
    }
    .map(word => (word, 1)).reduceByKey(_ + _).map(x =>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
	counts.collect()*/
    sc.stop()
  }
}











