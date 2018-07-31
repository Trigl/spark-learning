package ink.baixin.spark.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
  *
  * Usage: NetworkWordCount <hostname> <port> <checkpoint-directory>
  * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
  * <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example org.apache.spark.examples.streaming.WordCountByWindow1 localhost 9999 /Users/will/checkpoint/`
  */
object WordCountByWindow1 {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: NetworkWordCount <hostname> <port> <checkpoint-directory>")
      System.exit(1)
    }

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountByWindow1")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(args(2))


    // Create a DStream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(4), Seconds(2))
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}