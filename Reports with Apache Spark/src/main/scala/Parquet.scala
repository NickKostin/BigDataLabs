import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Parquet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop3.3.0\\distr\\hadoop")

    val cfg = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(cfg)
    sc.setLogLevel("error")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val postsRDD = sc.textFile("posts_sample.xml")
    val postsCount = postsRDD.count
    val yearAndTags = postsRDD.zipWithIndex().filter{case (elem, index) => index > 2 && index < postsCount - 1}.map(_._1).map(scala.xml.XML.loadString).flatMap(parsePost).cache()
    spark.createDataset(yearAndTags).write.parquet("post_dataset")
  }

  def parsePost(xml: scala.xml.Elem): Array[(Int, String)] = {
    val xmlDate = xml.attribute("CreationDate")
    val xmlTags = xml.attribute("Tags")
    if (xmlDate.isEmpty || xmlTags.isEmpty){
      return new Array[(Int, String)](0)
    }
    val creationDate = xmlDate.mkString
    val tags = xmlTags.mkString
    val year = creationDate.substring(0, 4)
    val tagsArray = tags.substring(4, tags.length-4).split("&gt;&lt;")

    tagsArray.map(
      tag => (year.toInt, tag)
    )
  }


}
