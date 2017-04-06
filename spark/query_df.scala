val logFile = "hdfs:///user/root/data.csv"

case class AccessLogRecord (
    clientIpAddress: String,
    domainName: String,
    remoteUser: String,
    monthDate: String,
    request: String,
    httpStatusCode: String,
    bytesSent: String,
    referer: String,
    userAgent: String
)

val df1 = sc.textFile(logFile).
    map(line => line.split(";").
    map(_.trim)).
    map(t => AccessLogRecord(t(0), t(1), t(2), t(3), t(4), t(5), t(6), t(7), t(8))).
    toDF
df1.registerTempTable("df1")
df1.show(false)
/*
val df2 = sqlContext.sql(
      "SELECT DISTINCT monthDate, clientIpAddress " +
      "FROM df1 " +
      "WHERE domainName = 'www.mydemo.fr'")
df2.registerTempTable("df2")

val df3 = sqlContext.sql(
      "SELECT monthDate, count(clientIpAddress) as total " +
      "FROM df2 " +
      "GROUP BY monthDate " +
      "ORDER BY monthDate ASC " +
      "LIMIT 10")
df3.show(false)
*/
