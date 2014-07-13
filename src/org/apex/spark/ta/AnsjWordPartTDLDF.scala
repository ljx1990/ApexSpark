package org.apex.spark.ta

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.ansj.splitWord.analysis.ToAnalysis
import scala.Array.canBuildFrom
class AnsjWordPartTDLDF {

}

/**
 * 通过开源的Ansj分词技术
 * 基于词典的分词
 *
 * hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt  投诉内容描述
 * hdfs://localhost:8020/user/lvjx/data/lvjx/dict.txt  投诉关键字
 */
object AnsjWordPartTDLDF {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "WordCount",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    //val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/aoctext.txt")

    //val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my.txt")

    /**
     * 经过清洗过的,将DOC换行符统一替换
     */
    val file = sc.textFile("hdfs://localhost:8020/user/lvjx/data/lvjx/my_clean.txt", 30)
      .filter(line => (!line.equals("")))
    val doc_num = file.count;
    /**
     * 计算文档数IDF
     */
    val idf = file.flatMap {
      line => ToAnalysis.simpleParse(line).toArray().distinct.filter(word => if (word.toString().matches("^[0-9]+$")) false else true)
    }.map(word => (word, 1)).reduceByKey(_ + _).map(x => (x._1, Math.log(doc_num / x._2))).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    /**
     * 必须要加collect,以执行文件处理
     */
    val idf_map = idf.collect().toMap
    //idf.saveAsTextFile("hdfs://localhost:8020/user/lvjx/data/lvjx/idf")
    /**
     * 计算文档数TFIDF
     */
    var row_id = 0;
    val tfidf = file.flatMap {
      line =>
        /**
         * 分词数组   ---加载用户自定义词典会出内存溢出,需要优化(无关)
         */
        val word_array = ToAnalysis.simpleParse(line).toArray().filter(word=>if (word.toString().matches("^[0-9]+$")) false else true);
        /**
         * key 字符 ->  (key行数  -> value(TF值))
         */
        row_id = row_id + 1
        /**
         * 计算行内词频TF  并乘以IDF
         *
         */
        val ss = word_array.map(word => (word, 1))
          .groupBy(_._1)
          .map {
            case (which, counts) =>
              (which, counts.foldLeft(0)(_ + _._2))
          }.toList
          .map(x=>(row_id,(x._1,(idf_map(x._1))*x._2/int2float(word_array.size))))
          .filter(map=>(map._2._2>0.02)).sortBy(_._2._2).reverse
        ss
        //val ss = word_array.map(word => (word, 1))
    }
    
    /**
     * TF-IDF最终结果
     */
    tfidf.saveAsTextFile("hdfs://localhost:8020/user/lvjx/data/lvjx/tfidf")
    sc.stop()
  }
}
//val word_array = ToAnalysis.simpleParse("市场流量通话包,流量").toArray();
//ToAnalysis.simpleParse("今天是个好日子15757455562").toArray().distinct.filter(word=>if (word.toString().matches("^[0-9]+$")) false else true)
//"123456".equals("/^[^0-9]*$/")





