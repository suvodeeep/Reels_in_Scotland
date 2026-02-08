version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // Core Flink streaming API
  "org.apache.flink" %% "flink-streaming-scala" % "1.18.1",
  "org.apache.flink" % "flink-connector-base" % "1.18.1",
  
  // Flink Kafka connector
  "org.apache.flink" % "flink-connector-kafka" % "3.2.0-1.18",
  
  // JSON serialization/deserialization
  "org.apache.flink" % "flink-json" % "1.18.1",
  
  // MongoDB connector
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.11.0",
  
  // For local testing and execution
  "org.apache.flink" % "flink-clients" % "1.18.1",
  "org.apache.flink" % "flink-core" % "1.18.1",
  "org.apache.flink" % "flink-runtime-web" % "1.18.1",
  
  // Log4j2 for unified logging (replaces slf4j-simple)
  "org.apache.logging.log4j" % "log4j-api" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1"
)

// Exclude competing logging frameworks
excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12"),
  ExclusionRule("org.slf4j", "slf4j-simple"),
  ExclusionRule("log4j", "log4j"),
  ExclusionRule("ch.qos.logback", "logback-classic"),
  ExclusionRule("ch.qos.logback", "logback-core")
)

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "-Dlog4j2.configurationFile=src/main/resources/log4j2.properties"
)

// Java 11 bytecode compatibility
javacOptions ++= Seq("-source", "11", "-target", "11")

// Assembly plugin settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Main class for execution
Compile / mainClass := Some("WeatherStreamProcessor")