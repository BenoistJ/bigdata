# exo1
val rdd1 = sc.textFile("e://demo/data.csv")
rdd1.count

#exo2
val rdd1 = sc.textFile("e://demo/data.csv")
val rdd2 = rdd1.filter(line => line.contains("www.mydemo.fr"))
val rdd3 = rdd2.map(line => line.split(";"))
val rdd33 = rdd3.coalesce(4)
val rdd4 = rdd3.map(cols => (cols(0), 1))
val rdd5 = rdd4.reduceByKey(_ + _)
val rdd6 = rdd5.sortBy(_._2, ascending=false)
rdd6.foreach(println)

rdd6.coalesce(1).foreach(println)
rdd6.collect.foreach(println)

#exo3
case class AccessLogRecord (
    clientIpAddress: String,
    domainName: String,
    monthDate: String,
    request: String
)
val df1 = sc.textFile("e://demo/data.csv").
    map(_.split(";")).
    map(t => AccessLogRecord(t(0), t(1), t(2), t(3))).
    toDF
df1.registerTempTable("toto")
val df2 = sql("SELECT clientIpAddress, count(*) as total FROM toto WHERE domainName='www.mydemo.fr' GROUP BY clientIpAddress ORDER BY total DESC")
df2.show
