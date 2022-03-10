import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// Define I/O case classes
case class OrderEvent(orderId: Long, eventType: String, transactionId: String, timestamp: Long)
case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Step 0 Read data from file
    //  val resource = getClass.getResource("/OrderLog.csv")
    //  val orderEventStream = env.readTextFile(resource.getPath)
      val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.orderId)

    // Step 1. Define a specific pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // Step 2. Apply this pattern to data stream and conduct pattern detection
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // Step 3. Define output tag to process timeout event
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    // Step 4. Invoke select method to extract and process timeout event and successful event
    val resultStream = patternStream.select( orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

// Self-defined PatternTimeoutFunction and PatternSelectFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout " + ": +" + timeoutTimestamp)
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
