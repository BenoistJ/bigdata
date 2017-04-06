val logFile = "hdfs:///user/root/data.csv"

val rdd1 = sc.textFile(logFile).
    map(line => line.split(";").
    map(_.trim))

val rdd2 = rdd1.
  filter(alr => alr(1) == "www.mydemo.fr").
  map(alr => (alr(3), alr(0))).
  distinct().
  map{ case ((monthDate, _)) => (monthDate, 1) }.
  reduceByKey(_ + _).
  sortBy(_._1, ascending=true)

rdd2.take(10).foreach(println)

