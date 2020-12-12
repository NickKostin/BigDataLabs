import java.time.LocalDateTime

case class Trip(
  tripId:Integer,
  duration:Integer,
  startDate:LocalDateTime,
  startStation:String,
  startTerminal:Integer,
  endDate:LocalDateTime,
  endStation:String,
  endTerminal:Integer,
  bikeId: Integer,
  subscriptionType: String,
  zipCode: String
) extends Ordered[Trip]{
  override def compare(that: Trip): Int = this.startDate.compareTo(that.startDate)
}
