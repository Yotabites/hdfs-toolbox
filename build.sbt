name := "hdfs-toolbox"

version := "1.0"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-client" % "1.2.3" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"
)
