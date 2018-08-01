package ink.baixin.spark.examples.streaming

import java.net.URI
import scala.collection.JavaConverters._
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

/**
  * Utility functions for Spark Streaming examples.
  */
object KinesisExampleUtils extends Logging {
  def getRegionNameByEndpoint(endpoint: String): String = {
    val uri = new URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }

  // Set reasonable logging levels for streaming if the user has not configure log4j.
  def setStreamingLogLevels(): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging,
      // then we override the logging level.
      logInfo("Setting log level to [WARN] for streaming examples." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
