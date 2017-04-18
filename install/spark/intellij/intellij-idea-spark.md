# How to setup a Spark environment with Intellij IDEA

| Software       | Tested version     |
|----------------|--------------------|
| Intellij IDEA  | Community 2016.3.2 |
| Scala          | 2.11.8             |
| Spark          | 2.1.0              |
| Linux          | Ubuntu 14.04       |
| Windows        | 7                  |

## Set up the Intellij IDEA project

- Install [Intellij IDEA](https://www.jetbrains.com/idea/download/#section=windows)
- Install the [JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- Launch it and stay at the welcome screen. If you have already launched a project, use `File -> Close Project`
- Install the **Scala** Intellij IDEA plugin (`Configure -> Plugin -> Install Jet Brain plugins...` and search `Scala`)
- Create a new project and select `Maven`. Select your JDK folder installation.
- Check the `Create from archetype` checkbox and lick on the `Add archetype...` button:
    - Groupe ID: `net.alchim31.maven`
    - ArtefactID: `scala-archetype-simple`
    - Version: `1.6`
- Then, you will specify your project's GroupID (ex: `tfolliot.scala`) and ArtefactID (ex: `spark-poc`)
- Once the project is created, you will see an alert. Click on `Enable auto-import`. Wait a minute while Maven is downloading and building the base app.
- Edit the `pom.xml` as follow:
    - Remove the `<arg>-make:transitive</arg>` line
    - Change the test dependencies with:
    
      ```xml
      <!-- Test -->
      <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>4.12</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.specs2</groupId>
        <artifactId>specs2-core_${scala.compat.version}</artifactId>
        <version>3.7.2</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.specs2</groupId>
        <artifactId>specs2-junit_${scala.compat.version}</artifactId>
        <version>3.7.2</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest_${scala.compat.version}</artifactId>
        <version>2.2.6</version>
        <scope>test</scope>
      </dependency>
      ```

Your project is now initialized. By right clicking on your project root, you can run the unit test (`Run -> All tests`). The App class contains an hello world code (run it with `Ctrl + Maj + F10`):
```scala
object App {
  def main(args : Array[String]) {
    println( "Hello World!" )
  }
}
```

## Install Apache Spark
Simply add the following dependency:
```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_${scala.compat.version}</artifactId>
  <version>2.1.0</version>
</dependency>
```
You can test your setup with this very simple code, which remove all even numbers:
```scala
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))
    val rdd = sc.parallelize(Array(1, 2, 3))
    rdd.filter(_ % 2 == 1)
       .foreach(println(_))
  }
}
```
## Install Spark Streaming
Similarly, you just have to add the dependency:
```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming_${scala.compat.version}</artifactId>
  <version>2.1.0</version>
</dependency>
```

You can now use the library:

```scala
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(1))

    val dstream = ssc.socketTextStream("localhost", 9999)
                     .flatMap(_.split(" "))
                     .map((_, 1))
                     .reduceByKey(_ + _)
    dstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
```

## Install MLib
Import the following dependency:

```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-mllib_${scala.compat.version}</artifactId>
  <version>2.1.0</version>
</dependency>
```