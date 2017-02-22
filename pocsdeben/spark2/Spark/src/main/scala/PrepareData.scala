import com.alvinalexander.accesslogparser._
import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.hadoop.fs._

object PrepareData {
  def main(args: Array[String]) {
      println("[PrepareData module]")

      val logFile = new Path("hdfs:///user/ubuntu/full/raw/*.log")
      val csvFiles = new Path("hdfs:///user/ubuntu/full/prepared/temp")
      val csvSingleFile = new Path("hdfs:///user/ubuntu/full/prepared/data.csv")

      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("Remoting").setLevel(Level.OFF)

      val conf = new SparkConf()
        .setAppName("PrepareData")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[AccessLogParser], classOf[AccessLogRecord]))
      val sc = new SparkContext(conf)

      val hdfs = FileSystem.get(sc.hadoopConfiguration)
      if (hdfs.exists(csvFiles)) {
        println("Deleting existing folder.")
        hdfs.delete(csvFiles, true)
      }

      val sw = new StopWatch
      sw.start()

      println("Converting Apache log files to csv files.")
      val parser = new AccessLogParser
      val rdd = sc.textFile(logFile.toUri.toString)
        .map(parser.parseLog)
        .filter(alr => Utils.ParseDate(alr.dateTime) != "2015-12")
        .map(alr =>
            alr.clientIpAddress + ";" +
            Utils.ConvertDomain(alr.domainName) + ";" +
            alr.remoteUser + ";" +
            Utils.ParseDate(alr.dateTime) + ";" +
            Utils.ParseUrl(alr.request) + ";" +
            alr.httpStatusCode + ";" +
            alr.bytesSent + ";" +
            /*alr.referer*/"-" + ";" +
            /*alr.userAgent*/"-")
      rdd.saveAsTextFile(csvFiles.toString)

      println("Merging to one big csv file.")
      if (hdfs.exists(csvSingleFile)) {
        println("Deleting output file.")
        hdfs.delete(csvSingleFile, true)
      }
      //FileUtil.copyMerge(hdfs, new Path(csvFiles), hdfs, new Path(csvSingleFile), false, sc.hadoopConfiguration, null)
      HadoopUtils.copyMergeWithHeader(hdfs, csvFiles, hdfs, csvSingleFile, false, sc.hadoopConfiguration, "clientIpAddress;domainName;remoteUser;monthDate;request;httpStatusCode;bytesSent;referer;userAgent")

      sw.stop()
      val elapsedTime = (sw.getTime / 1000).asInstanceOf[Int]
      println()
      println(s"Job finished after ${elapsedTime} seconds.")
    }
}
