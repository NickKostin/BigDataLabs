case class Station (
  stationId:Integer,
  name:String,
  lat:Double,
  long:Double,
  dockcount:Integer,
  landmark:String,
  installation:String,
  notes:String
){
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
}
