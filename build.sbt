name := "spark-local-extrema"

organization := "com.github.thomashan"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.11"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0" % "provided",

  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.4" % "test"
)

parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
