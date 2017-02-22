import com.alvinalexander.accesslogparser.AccessLogRecord
import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Rdd2 {
  def main(args: Array[String]) {
    println()
    println("[Rdd2 module]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("Remoting").setLevel(Level.OFF)

    val logFile = "hdfs:///user/ubuntu/full/prepared/data.csv"

    val conf = new SparkConf()
      .setAppName("Rdd")
    val sc = new SparkContext(conf)

    val sw = new StopWatch
    sw.start()

    val rdd1 = sc.textFile(logFile)
      .map(line => line.split(";").map(_.trim))
      .map(t => AccessLogRecord(t(0), t(1), t(2), t(3), t(4), t(5), t(6), t(7), t(8)))

    val rdd2 = rdd1
      .filter(alr => alr.domainName == "www.mywebsite.com")
      .map(alr => (alr.dateTime, alr.clientIpAddress))
      .distinct()
      .map{ case ((date, _)) => (date, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._1, ascending=true)
    println("------ Nombre d'adresses IP uniques par mois :")
    rdd2.collect().foreach(println)

    sw.stop()
    val elapsedTime = (sw.getTime / 1000).asInstanceOf[Int]
    println(s"Job finished after $elapsedTime seconds.")
    //Iterator.continually(Console.readLine).takeWhile(_.nonEmpty).foreach(line => println("read " + line))
  }
}
