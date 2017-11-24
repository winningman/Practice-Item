package per.wanghai

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangHai on 2017/11/22 21:43
  */
object WordcountCluster {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("WordCountScala")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val wordcounts = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wordcounts.foreach(wordcount => println(wordcount._1 + "出现了" + wordcount._2 + "次。"))
    val wordsort = wordcounts.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    wordsort.saveAsTextFile(args(1))
    sc.stop()
  }
}
