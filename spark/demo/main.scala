val rdd1 = sc.textFile("e://demo/data.csv")
rdd1.count

val rdd2 = rdd1.filter(_.contains("www.mydemo.fr"))
val rdd3 = rdd2.map(_.split(";"))
val rdd33 = rdd3.coalesce(4)
val rdd4 = rdd3.map(cols => (cols(0), 1))
val rdd5 = rdd4.reduceByKey(_ + _)
val rdd6 = rdd5.sortBy(_._2, ascending=false)
rdd6.foreach(println)

rdd6.coalesce(1).foreach(println)
rdd6.collect.foreach(println)
