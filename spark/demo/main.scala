val rdd1 = sc.textFile("e://demo/data.csv")
rdd1.count

val rdd2 = rdd1.filter(_.contains("www.mydemo.fr"))
val rdd3 = rdd2.map(_.split(";"))
val rdd4 = rdd3.coalesce(4)
val rdd5 = rdd4.map(cols => (cols(0), 1))
val rdd6 = rdd5.reduceByKey(_ + _)
val rdd7 = rdd6.sortBy(_._2, ascending=false)
rdd7.take(10).foreach(println)
