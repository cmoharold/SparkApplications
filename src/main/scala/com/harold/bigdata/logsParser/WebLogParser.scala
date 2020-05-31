package com.harold.bigdata.LogParser

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import java.util.regex.{Matcher, Pattern}

import com.harold.bigdata.utils.Utilities._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/** Maintains top URL's visited over a 5 minute window, from a stream
  *  of Apache access logs on port 9999.
  */
object WebLogParser {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc: StreamingContext = new StreamingContext("local[*]", "LogParser", Seconds(1))

    val homeDir = System.getProperty("user.home")

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern: Pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    // In console: netcat -kl 9999 < access_log.txt
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

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
