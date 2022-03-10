import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// Define the output Uv statistics sample class
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read data from a file
    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // Convert to the sample class type and extract the timestamp and watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll( Time.hours(1) )     // There is no grouping directly, and the 1-hour rolling window is opened based on DataStream
      .apply( new UvCountResult() )

    uvStream.print()

    env.execute("uv job")
  }
}

// Custom implementation of the full window function, with a Set structure to save all user ids, automatic deduplication
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // define a Set
    var userIdSet = Set[Long]()

    // Iterate over all the data in the window, add the userId to the set, automatically deduplicate
    for( userBehavior <- input )
      userIdSet += userBehavior.userId

    // Print the size of the set as the duplicated UV value
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}
