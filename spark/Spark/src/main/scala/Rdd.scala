import com.alvinalexander.accesslogparser._
import org.apache.commons.lang.time.StopWatch
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Rdd {
  def main(args: Array[String]) {
    println()
    println("[Rdd module]")

    // http://stackoverflow.com/questions/29208844/apache-spark-logging-within-scala
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("Remoting").setLevel(Level.OFF)

    val logFile = "hdfs:///user/ubuntu/full/prepared/data.csv"

    val conf = new SparkConf()
      .setAppName("Rdd")
      //.setMaster("local[8]")
      //.set("spark.ui.showConsoleProgress", "false")
    val sc = new SparkContext(conf)

    val sw = new StopWatch
    sw.start()

    val rdd1 = sc.textFile(logFile)
      .map(line => line.split(";").map(_.trim))
      //.cache
    //val numInputs = rdd1.count

    /*
    val rdd2 = rdd1
      .filter(alr => alr(1) == "www.mywebsite.com")
      .map(alr => (alr(4), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending=false)
    println("------ Les 10 pages Web les plus visités :")
    rdd2.take(10).foreach(println)
    */

    val rdd3 = rdd1
      .filter(alr => alr(1) == "www.mywebsite.com")
      .map(alr => (alr(3), alr(0)))
      .distinct()
      .map{ case ((date, _)) => (date, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._1, ascending=true)
    println("------ Nombre d'adresses IP uniques par mois :")
    rdd3.collect().foreach(println)
    /*
    // Second solution
    val rdd2 = rdd1
      .filter(alr => alr(1) == "www.mywebsite.com")
      .map(alr => (alr(3), alr(0)))
      .groupByKey()
      .mapValues(_.size)
      .sortBy(_._1, false)
    */

    sw.stop()
    val elapsedTime = (sw.getTime / 1000).asInstanceOf[Int]
    println()
    //println(s"Job finished after $elapsedTime seconds. input.Count=$numInputs")
    println(s"Job finished after $elapsedTime seconds.") 
    //Iterator.continually(Console.readLine).takeWhile(_.nonEmpty).foreach(line => println("read " + line))
  }
}
