import com.alvinalexander.accesslogparser.AccessLogRecord
import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Dataframe {
  def main(args: Array[String]) {
    println()
    println("[Dataframe module]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("Remoting").setLevel(Level.OFF)

    val logFile = "hdfs:///user/ubuntu/light/prepared/data.csv"
    val conf = new SparkConf()
      .setAppName("Dataframe")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val sw = new StopWatch
    sw.start()

    val df1 = sc.textFile(logFile)
      .map(line => line.split(";").map(_.trim))
      .map(t => AccessLogRecord(t(0), t(1), t(2), t(3), t(4), t(5), t(6), t(7), t(8)))
      .toDF
      .cache

    /*
    val df1 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")             // Use first line of all files as header
      .option("inferSchema", "false")       // Automatically infer data types
      .load(logFile)
      .cache
    */
    df1.registerTempTable("df1")

    val df2 = sqlContext.sql(
      "SELECT DISTINCT dateTime, clientIpAddress " +
      "FROM df1 " +
      "WHERE domainName = 'www.mywebsite.com'")
    df2.registerTempTable("df2")
  
    val df3 = sqlContext.sql(
      "SELECT dateTime, count(clientIpAddress) as c " +
      "FROM df2 " +
      "GROUP BY dateTime " +
      "ORDER BY dateTime ASC")
    println("Nombre d'adresses IP uniques par mois :")
    df3.show(false)

    sw.stop()
    val elapsedTime = (sw.getTime / 1000).asInstanceOf[Int]
    println()
    println(s"Job finished after ${elapsedTime} seconds.")

    //Iterator.continually(Console.readLine).takeWhile(_.nonEmpty).foreach(line => println("read " + line))
  }
}
