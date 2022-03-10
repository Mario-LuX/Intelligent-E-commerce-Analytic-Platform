
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// Input login event sample class
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
// Output alarm information sample class
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Read data
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // Convert to sample class type and mention timestamp and watermark
    val loginEventStream = inputStream
      .map( data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // Judgment and detection, if continuous login fails within 2 seconds, output alarm information
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process( new LoginFailWarningResult(2) )

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // Define the state, save all current login failure events, and save the timestamp of the timer
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // Determine whether the current login event is a success or a failure
    if( value.eventType == "fail" ){
      loginFailListState.add(value)
      // If there is no timer, then register a timer after 2 seconds
      if( timerTsState.value() == 0 ){
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // If it is successful, then clear the state and timer directly and start over
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      allLoginFailList += iter.next()
    }
    // Determine the number of login failure events, if it exceeds the upper limit, alarm
    if(allLoginFailList.length >= failTimes){
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
          "login fail in 2s for " + allLoginFailList.length + " times."
        ))
    }

    // Clear state
    loginFailListState.clear()
    timerTsState.clear()
  }
}