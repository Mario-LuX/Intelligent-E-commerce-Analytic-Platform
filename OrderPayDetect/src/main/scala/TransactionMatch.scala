import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// Define case class of the event of receiving money
case class ReceiptEvent(transactionId: String, payChannel: String, timestamp: Long)

object TransactionMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. Read order log
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource1.getPath)
      //    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val arr = data.split(",")
      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.eventType == "pay")
      .keyBy(_.transactionId)

    // 2. Read receipt log
    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(resource2.getPath)
      //    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
      val arr = data.split(",")
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.transactionId)

    // 3. Join two streams
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process( new TransactionPayMatchResult() )

    resultStream.print("matched")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched pays")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipts")

    env.execute("transaction match job")
  }
}


class TransactionPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // Define states to store the information of paying an order and receiving money of this order
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))
  // Define output tag
  val unmatchedPayEventOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
  val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")


  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // When there's a 'pay' event, check whether corresponding acount receives the money ('receipt' event)
    val receipt = receiptEventState.value()
    if( receipt != null ){
      // A 'receipt' event exists, successful transaction
      out.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else{
      // No 'receipt' event, set the timer from this 'pay' event and wait for 5 seconds
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      // Update states
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // We have a 'receipt' event, determine whether a 'pay' event happened
    val pay = payEventState.value()
    if( pay != null ){
      // 'pay' event exists, successful transaction and clear states
      out.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    } else{
      // No 'pay' event exists, set the timer and wait for 3 seconds
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      // update states
      receiptEventState.update(receipt)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // Timer is triggered. In this case, for 'pay' and 'receipt' event
    // If one event type exists, this indicates the other type doesn't happen
    if( payEventState.value() != null ){
      ctx.output(unmatchedPayEventOutputTag, payEventState.value())
    }
    if( receiptEventState.value() != null ){
      ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
    }
    // clear states
    receiptEventState.clear()
    payEventState.clear()
  }
}