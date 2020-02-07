import AssemblyKeys._

assemblySettings

organization  := "com.hcl.optimus.context"

name := "optimus-common"

version := "1.0.1"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

val sparkversion = "2.3.2"

unmanagedJars in Compile += file("lib/XSLTProcessor-0.0.1-SNAPSHOT.jar")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkversion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkversion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkversion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkversion % "provided",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "commons-codec" % "commons-codec" % "1.12",
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  "com.typesafe" % "config" % "1.0.2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.6",
  "commons-net" % "commons-net" % "3.6",
  "com.jcraft" % "jsch" % "0.1.54",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "mysql" % "mysql-connector-java" % "5.1.31",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.yaml" % "snakeyaml" % "1.8",
  "org.apache.kafka" %% "kafka" % "0.8.2.1",
  "com.eaio.uuid" % "uuid" % "3.2",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.519",
  "org.json" % "json" % "20090211"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  //"Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
//  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
 // "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

