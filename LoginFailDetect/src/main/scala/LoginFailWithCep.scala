
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Read data
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // Convert to sample class type and mention timestamp and watermark
    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 1. Define a matching pattern, requiring a login failure event followed by another login failure event
    //        val loginFailPattern = Pattern
    //          .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
    //          .next("secondFail").where(_.eventType == "fail")
    //          .next("thirdFail").where(_.eventType == "fail")
    //          .within(Time.seconds(5))
    val loginFailPattern = Pattern
      .begin[LoginEvent]("fail").where(_.eventType == "fail").times(3).consecutive()
      .within(Time.seconds(5))

    // 2. Apply the pattern to the data stream to get a PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3. To detect a data stream that conforms to the pattern, you need to call select
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())

    loginFailWarningStream.print()

    env.execute("login fail with cep job")
  }
}

// Implement custom PatternSelectFunction
class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // The currently matched event sequence is saved in the Map
    //    val firstFailEvent = pattern.get("firstFail").iterator().next()
    //    val thirdFailEvent = pattern.get("thirdFail").get(0)
    val iter = pattern.get("fail").iterator()
    val firstFailEvent = iter.next()
    val secondFailEvent = iter.next()
    val thirdFailEvent = iter.next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")
  }
}
