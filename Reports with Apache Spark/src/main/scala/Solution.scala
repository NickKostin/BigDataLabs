import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Solution {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.rdd.RDD
    import scala.reflect.ClassTag
    import java.time.{Duration, LocalDateTime}

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop3.3.0\\distr\\hadoop")

    val cfg = new SparkConf()
      .setAppName("Test")
      .setMaster("local[6]")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory","6g")
      .set("spark.executor.instances", "1")
      .set("spark.executor.cores", "1")
    val sc0 = new SparkContext(cfg)
    sc0.stop()
    val sc = new SparkContext(cfg)
    sc.setLogLevel("error")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val posts_path = "posts.xml"
    val programming_languages_path = "data_programming-languages.csv"
    val years = 2008 to 2019 map(_.toString)
    val topCount = 5

    val startTime = LocalDateTime.now()

    val postsRDD = sc.textFile(posts_path).mapPartitions(_.take(1000))
    val posts_count = postsRDD.count
    val posts_raw = postsRDD.zipWithIndex.filter{ case (s, idx) => idx>2 && idx<posts_count-1 }.map(_._1)

    val programming_languages_raw = sc.textFile(programming_languages_path).zipWithIndex.filter{
      case (row, idx) => idx>1
    }.map{
      case (row, idx) => row
    }
    val programming_languages = programming_languages_raw.map{
      row => row.split(",")
    }.filter{
      rowValues => rowValues.size==2
    }.map{
      rowValues =>
        val Seq(name, link) = rowValues.toSeq
        name.toLowerCase
    }.collect()

    val posts_xml = posts_raw.map(row => scala.xml.XML.loadString(row))

    def extractCreationDateAndTags(e: scala.xml.Elem) = {
      val creationDate = e.attribute("CreationDate")
      val tags = e.attribute("Tags")
      (creationDate, tags)
    }
    val postCreationDateAndTags = posts_xml.map(extractCreationDateAndTags).filter {
      x => x._1.isDefined && x._2.isDefined
    }.map{
      x => (x._1.mkString, x._2.mkString)
    }

    def parseCreationDateAndTags(e:(String, String)) = {
      val (creationDate, tags) = e
      val year = creationDate.substring(0, 4)
      val tagsArray = tags.substring(4, tags.length-4).split("&gt;&lt;")
      (year, tagsArray)
    }
    val postYearTags = postCreationDateAndTags.map(parseCreationDateAndTags)

    val yearTags = postYearTags.flatMap{ case (year, tags) => tags.map(tag => (year, tag))}
    val yearLanguageTags = yearTags.filter{ case (year, tag) => programming_languages.contains(tag) }.cache()

    val yearsTagCounts = years.map{ reportYear =>
      yearLanguageTags.filter{
        case (tagYear, tag) => reportYear==tagYear
      }.map{
        case (tagYear, tag) => (tag, 1)
      }.reduceByKey{
        (a, b) => a + b
      }.map{ case (tag, count) =>
        (reportYear, tag, count)
      }
    }


    val topYearsTagCounts = yearsTagCounts.map{ yearTagsCounts =>
      yearTagsCounts.sortBy{ case (year, tag, count) => -count }.take(topCount)
    }

    val finalReport = topYearsTagCounts.reduce((a, b) => a.union(b))
    val finalDataFrame = sc.parallelize(finalReport).toDF("Year", "Language", "Count")

    val stopTime = LocalDateTime.now()

    finalDataFrame.show(years.size*topCount)
    println(s"duration ${ java.time.Duration.between(startTime, stopTime) }" )
  }
}
