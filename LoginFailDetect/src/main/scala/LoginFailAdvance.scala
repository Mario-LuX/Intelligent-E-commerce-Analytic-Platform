
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailAdvance {
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
      .process( new LoginFailWaringAdvanceResult() )

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWaringAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // Define the state, save all current login failure events, and save the timestamp of the timer
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // First determine the event type
    if( value.eventType == "fail" ){
      // 1. If it fails, make further judgments
      val iter = loginFailListState.get().iterator()
      // Determine if there is a previous login failure event
      if(iter.hasNext){
        // 1.1 If so, then judge the time difference between the two failures
        val firstFailEvent = iter.next()
        if( value.timestamp < firstFailEvent.timestamp + 2 ){
          // If within 2 seconds, output an alarm
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // Regardless of whether the alarm is reported or not, it has been processed at present, and the status is updated to the events that failed to log in in sequence recently.
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 If not, add the current event directly to the ListState
        loginFailListState.add(value)
      }
    } else {
      // 2. If it is successful, clear the state directly
      loginFailListState.clear()
    }
  }
}