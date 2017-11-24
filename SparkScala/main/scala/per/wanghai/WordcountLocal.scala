package per.wanghai

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by WangHai on 2017/11
  */
object WordcountLocal {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("WordCountScala")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    // D:/sbt/myproject/SparkScala/2.txthdfs://host0.com:8020/user/attop/test_data/SPARK_README.md
    val lines = sc.textFile("D:/sbt/myproject/SparkScala/2.txt", 1)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordcounts = pairs.reduceByKey {
      _ + _
    }
    wordcounts.foreach(wordcount => println(wordcount._1 + "出现了" + wordcount._2 + "次。"))
  }
}
