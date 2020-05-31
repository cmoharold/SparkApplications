package com.harold.bigdata.LogParser

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerConfig

import kafka.serializer.StringDecoder

import java.util.regex.{Matcher, Pattern}

import com.harold.bigdata.utils.Utilities._

/** Working example of listening for log data from Kafka's testLogs topic on port 9092.
  *  >kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testLogs
  *  >kafka-console-producer --broker-list localhost:9092 --topic testLogs < access_log.txt
  */
object WebLogParserWithKafka {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc: StreamingContext = new StreamingContext("local[*]", "logsParserWithKafka", Seconds(1))

    val homeDir = System.getProperty("user.home")

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern: Pattern = apacheLogPattern()

    // hostname:port for Kafka brokers
    //val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> "localhost:9092")
    val kafkaParams: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG-> "localhost:9092",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG-> "false",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG-> "smallest"
    )
    // List of topics you want to listen for from Kafka
    val topics: Set[String] = List("testLogs").toSet
    // Create our Kafka stream, which will contain (topic,message) pairs. We tack a
    // map(_._2) at the end in order to only get the messages, which contain individual
    // lines of data.
    val lines: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    // Extract the request field from each log line
    val requests: DStream[Any] = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

    // Extract the URL from the request
    val urls: DStream[String] = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts: DStream[(String, Int)] = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults: DStream[(String, Int)] = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint(homeDir+"/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

