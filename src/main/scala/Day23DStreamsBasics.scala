
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val conf = new SparkConf()
      .setAppName("Day23-DStreams-Basics")
      .setMaster("local[*]")

    // 2. Create StreamingContext with a 5-second batch interval
    val ssc = new StreamingContext(conf, Seconds(5))

    // 3. Read application logs from a socket
    val logs = ssc.socketTextStream("localhost", 9999)

    // 4. Apply map, filter and flatMap transformations
    val processedLogs = logs
      .map(line => line.trim)
      .filter(line => line.nonEmpty)
      .map(line => (line, line.toUpperCase))
      .filter { case (_, upperLine) =>
        upperLine.contains("ERROR")
      }
      .flatMap { case (originalLine, _) =>
        Seq(originalLine)
      }

    // 5. Count ERROR messages in each micro-batch
    val errorCounts = processedLogs.count()

    // 6. Print the count for each batch
    errorCounts.print()

    // Start streaming
    ssc.start()

    // Keep the streaming application running
    ssc.awaitTermination()
  }
}
