import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop3.3.0\\distr\\hadoop")

    val cfg = new SparkConf()
      .setAppName("Test")
      .setMaster("local[8]")
    val sc = new SparkContext(cfg)
    sc.setLogLevel("error")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val years = 2010 to 2020

    val topCount = 10

    val startTime = System.currentTimeMillis()

    val programming_languages = sc.textFile("data_programming-languages.csv")
      .zipWithIndex
      .filter{case (row, idx) => idx > 1}
      .map{case (row, idx) => row}
      .flatMap(parseLanguage)
      .collect()

    val postsRDD = sc.textFile("../posts.xml").persist(StorageLevel.DISK_ONLY)
    val postsCount = postsRDD.count
    val posts = postsRDD
      .zipWithIndex()
      .filter{case (elem, index) => index > 2 && index < postsCount - 1}
      .map(_._1)
      .map(scala.xml.XML.loadString)
      .flatMap(parsePost)
      .filter(post => programming_languages.contains(post.tag) && years.contains(post.year))
      .map(post => (post, 1))
      .reduceByKey(_+_)
      .map{case (post, count) => (post.year, (post.tag, count))}
      .groupByKey()
      .flatMapValues(_.toSeq.sortBy(-_._2).take(topCount))
      .map{case (year, (tag, count)) => (year, tag, count)}

    val finalDataFrame = posts.toDF("Year", "Language", "Count").sort(asc("Year"), desc("Count"))

    val endTime = System.currentTimeMillis()

    finalDataFrame.show(years.size * topCount)
    printf("duration = %d ms", endTime - startTime)
    sc.stop()
  }

  def parsePost(xml: scala.xml.Elem): Array[Post] = {
    val xmlDate = xml.attribute("CreationDate")
    val xmlTags = xml.attribute("Tags")
    if (xmlDate.isEmpty || xmlTags.isEmpty) {
      return new Array[Post](0)
    }
    val creationDate = xmlDate.mkString
    val tags = xmlTags.mkString
    val year = creationDate.substring(0, 4)
    val tagsArray = tags.substring(4, tags.length - 4).split("&gt;&lt;")

    tagsArray.map(
      tag => Post(year.toInt, tag)
    )
  }

  def parseLanguage(row: String): Array[String] = {
    val words = row.split(",")
    if (words.length == 2) {
      return Array(words(0).toLowerCase)
    }
    new Array[String](0)
  }


}
