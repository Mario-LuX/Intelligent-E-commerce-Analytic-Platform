import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

// Define the input data sample class
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
// Define the output data sample class
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)


// Custom test data source
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior]{
  // Whether to run the flag
  var running = true
  // Define a collection of user behaviors and channels
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    // Define a maximum amount of generated data
    val maxCounts = Long.MaxValue
    var count = 0L

    // while loop, keep randomly generating data
    while( running && count < maxCounts ){
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }
}

object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource( new SimulatedSource )
      .assignAscendingTimestamps(_.timestamp)

    // Windowing statistics output
    val resultStream = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow( Time.days(1), Time.seconds(5) )
      .process(new MarketCountByChannel())

    resultStream.print()

    env.execute("app market by channel job")

  }
}

// Custom ProcessWindowFunction
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketViewCount(start, end, channel,behavior, count))
  }
}
