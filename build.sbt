name := "hadoop-test-fs"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "provided"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

// Check deprecation without manual restart
javacOptions in ThisBuild ++= Seq("-Xlint:unchecked")
scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

// Display full-length stacktraces from ScalaTest
testOptions in Test += Tests.Argument("-oF")
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "+q")

parallelExecution in Test := false
