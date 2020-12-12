import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.lang.Math._

import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    System.setProperty("hadoop.home.dir", "D:\\hadoop3.3.0\\distr\\hadoop")

    val Seq(masterURL) = args.toSeq
    val cfg = new SparkConf().setAppName("Test").setMaster(masterURL)
    val sc = new SparkContext(cfg)
    sc.setLogLevel("error")

    val tripData = sc.textFile("trips.csv")
    printHeader(tripData) // в файле отсутствует заголовок
    val trips = tripData.map(row => parseTrip(row.split(",", -1)))

    val stationData = sc.textFile("stations.csv")
    printHeader(stationData) // в файле отсутствует заголовок
    val stations = stationData.map(row => parseStation(row.split(",", -1)))

    //joinTest(trips, stations)

    val tripsByStartStation = trips.keyBy(record => record.startStation)
    val tripsByEndStation = trips.keyBy(record => record.endStation)

    //groupByKeyTest(tripsByStartStation)
    //aggregateByKeyTest(tripsByStartStation)

    findBikeWithMaxDuration(trips, stations)
    // Find max distance in degree
    findStationsWithMaxDistance(trips, stations)
    findWayOfBikeWithMaxDuration(trips, stations)
    findCountOfBikes(trips, stations)
    findUsersWhoSpentMoreThan3HoursOnTrips(trips, stations)

    sc.stop()
  }

  def parseTrip(row: Array[String]): Trip = {
    val timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd H:m")
    Trip(
      tripId = row(0).toInt,
      duration = row(1).toInt,
      startDate = LocalDateTime.parse(row(2), timeFormat),
      startStation = row(3),
      startTerminal = row(4).toInt,
      endDate = LocalDateTime.parse(row(5), timeFormat),
      endStation = row(6),
      endTerminal = row(7).toInt,
      bikeId = row(8).toInt,
      subscriptionType = row(9),
      zipCode = row(10)
    )
  }

  def parseStation(row: Array[String]): Station = {
    Station(
      stationId = row(0).toInt,
      name = row(1),
      lat = row(2).toDouble,
      long = row(3).toDouble,
      dockcount = row(4).toInt,
      landmark = row(5),
      installation = row(6),
      notes = null
    )
  }

  def printHeader(rdd: RDD[String]): Unit = {
    println("\nprintHeader:")
    val header = rdd.first
    println(header)
  }

  def joinTest(trips: RDD[Trip], stations: RDD[Station]): Unit = {
    println("\njoinTest:")

    val stationsIndexed = stations.keyBy(record => record.stationId)
    val tripsByStartTerminals = trips.keyBy(record => record.startTerminal)
    val tripsByEndTerminals = trips.keyBy(record => record.startTerminal)

    val startTrips = stationsIndexed.join(tripsByStartTerminals)
    val endTrips = stationsIndexed.join(tripsByEndTerminals)

    printf("startTrips.count() = %d \n", startTrips.count())
    printf("endTrips.count() = %d \n", endTrips.count())
  }

  def groupByKeyTest(tripsByStartStation: RDD[(String, Trip)]): Unit = {
    println("\ngroupByKeyTest:")
    val start = System.currentTimeMillis()
    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x=>x.duration)
      .groupByKey()
      .mapValues(col=>col.reduce((a,b)=>a+b)/col.size)
    val time = System.currentTimeMillis() - start

    printf(">>> calculated by %d ms\n", time)
    avgDurationByStartStation.take(3).foreach(println)
  }

  def aggregateByKeyTest(tripsByStartStation: RDD[(String, Trip)]): Unit = {
    println("\naggregateByKeyTest")
    val start = System.currentTimeMillis()
    val avgDurationByStartStation = tripsByStartStation
      .mapValues(x=>x.duration)
      .aggregateByKey((0,0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1+acc2._1, acc1._2+acc2._2)
      ).mapValues(acc=>acc._1/acc._2)
    val time = System.currentTimeMillis() - start

    printf(">>> calculated by %d ms\n", time)
    avgDurationByStartStation.take(3).foreach(println)
  }

  def findBikeWithMaxDuration(trips: RDD[Trip], stations: RDD[Station]): Int = {
    println("\nfindBikeWithMaxDuration")
    val bikeWithDuration = trips
      .map(trip => (trip.bikeId, trip.duration))
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)
      .first
    printf("bikeId = %s, duration = %s\n", bikeWithDuration._1, bikeWithDuration._2)

    bikeWithDuration._1
  }

  /**
    * Find max distance in degree
    */
  def findStationsWithMaxDistance(trips: RDD[Trip], stations: RDD[Station]): Unit = {
    println("\nfindStationsWithMaxDistance")
    def distance(s1: Station, s2: Station): Double = sqrt(pow(s1.lat - s2.lat, 2) + pow(s1.long - s2.long, 2))

    val answer = stations
      .cartesian(stations)
      .map(pair => (pair._1.stationId, pair._2.stationId, distance(pair._1, pair._2)))
      .sortBy(_._3, ascending = false)
      .first

    printf("Max distance %f between %s and %s stations\n",
      answer._3,
      answer._1,
      answer._2)

  }

  def findWayOfBikeWithMaxDuration(trips: RDD[Trip], stations: RDD[Station]): Unit = {
    print("\nfindWayOfBikeWithMaxDuration")

    val bike = findBikeWithMaxDuration(trips, stations)
    trips
      .keyBy(_.bikeId)
      .lookup(bike)
      .sorted
      .foreach(
        trip => printf("%s %s ---> %s %s\n", trip.startStation, trip.startDate, trip.endStation, trip.endDate)
      )
  }

  def findCountOfBikes(trips: RDD[Trip], stations: RDD[Station]): Unit = {
    println("\nfindCountOfBikes")

    val count = trips
      .keyBy(_.bikeId)
      .groupByKey()
      .count

    printf("CountOfBikes = %d\n", count)
  }

  def findUsersWhoSpentMoreThan3HoursOnTrips(trips: RDD[Trip], stations: RDD[Station]): Unit = {
    println("\nfindUsersWhoSpentMoreThan3HoursOnTrips")

    trips
      .keyBy(_.zipCode)
      .mapValues(_.duration)
      .reduceByKey(_ + _)
      .filter(_._2 > 3 * 60 * 60)
      .foreach(
      v => printf("user \"%s\" spent %s seconds\n", v._1, v._2)
    )
  }
}
