name := "datamart"

version := "0.1"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.8.5",
  "com.typesafe.akka" %% "akka-stream" % "2.8.5",
  "com.typesafe.akka" %% "akka-http-core" % "10.5.3",
  "com.typesafe.akka" %% "akka-http" % "10.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-mllib" % "3.5.1",
  "ml.combust.bundle" %% "bundle-hdfs" % "0.23.1",
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1", // For automatic derivation of encoders and decoders
  "io.circe" %% "circe-parser" % "0.14.1"
)

// Updated syntax
fork := true
javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

// Add the repository for the Oracle JDBC driver
resolvers ++= Seq(
  "Akka Repository" at "https://repo.akka.io/releases/",
  "Apache Repository" at "https://repo1.maven.org/maven2/",
  //"Oracle Maven Repository" at "https://maven.oracle.com"
)

unmanagedJars in Compile += baseDirectory.value / "lib" / "clickhouse-native-jdbc-shaded-2.7.1.jar"