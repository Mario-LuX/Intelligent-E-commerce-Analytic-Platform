import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// Define input and output sample classes
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)
// Side output stream blacklist alarm information sample class
case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read data from file
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // Convert to sample class and extract timestamp and watermark
    val adLogStream = inputStream
      .map( data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // Insert a one-step filtering operation, and output users with brushing behavior to the side output stream (blacklist alarm)
    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FliterBlackListUserResult(100))

    // Windowed Aggregate Statistics
    val adCountResultStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate(new AdCountAgg(), new AdCountWindowResult())

    adCountResultStream.print("count result")
    filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("waring")
    env.execute("ad count statistics job")
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountWindowResult() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdClickCountByProvince(end, key, input.head))
  }
}

// KeyedProcessFunction
class FliterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]{
  // Define the state, save the user's clicks on the advertisement, clear the timestamp of the state at 0:00 every day, and mark whether the current user has entered the blacklist
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()

    // Judging that as long as the first data comes, directly register the clear state timer at 0 o'clock
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // Determine whether the count value has reached the defined threshold, and if it exceeds, output to the blacklist
    if( curCount >= maxCount ){
      // Determine whether it is already in the blacklist, if not, output the output stream on the side
      if(!isBlackState.value()){
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over " + maxCount + " times today."))
      }
      return
    }

    // Under normal circumstances, count is incremented by 1, and then the data is output as it is
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if(timestamp == resetTimerTsState.value()){
      isBlackState.clear()
      countState.clear()
    }
  }
}