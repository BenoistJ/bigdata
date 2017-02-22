name := "SparkTests"
version := "1.0"
scalaVersion := "2.10.5"
mainClass in Compile := Some("Dataframe")
unmanagedBase := baseDirectory.value / "lib"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"  withSources() withJavadoc()
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.3.0" withSources() withJavadoc()
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0"

// sbt package    <- pour générer un petit jar
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "benoist.jar" }

// sbt assembly   <- pour générer un jar avec toutes ses dépendences
assemblyJarName := "benoist.jar"
assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
  case n if n.startsWith("rootdoc.txt") => MergeStrategy.discard
  case n if n.startsWith("readme.html") => MergeStrategy.discard
  case n if n.startsWith("readme.txt") => MergeStrategy.discard
  case n if n.startsWith("library.properties") => MergeStrategy.discard
  case n if n.startsWith("license.html") => MergeStrategy.discard
  case n if n.startsWith("about.html") => MergeStrategy.discard
}