
name := "tobaccoProcess"

version := "0.1"

scalaVersion := "2.11.8"

resourceDirectory in Compile := baseDirectory.value / "resources"
resourceDirectory in packageBin := baseDirectory.value / "resources"
resourceDirectory in Test := baseDirectory.value / "test-resources"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.12"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
