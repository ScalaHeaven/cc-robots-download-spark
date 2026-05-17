ThisBuild / scalaVersion := "3.8.3"

ThisBuild / semanticdbEnabled := true

ThisBuild / scalacOptions += "-Yexplicit-nulls"

lazy val sparkVersion = "3.5.1"
lazy val jwarcVersion = "0.36.0"
lazy val sttpVersion = "3.5.2"
lazy val kafkaClientsVersion = "4.2.0"
lazy val munitVersion = "1.2.0"

lazy val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "cc-robots-download-spark",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      ("org.apache.spark" % "spark-sql_2.13" % sparkVersion)
        .exclude("commons-logging", "commons-logging"),
      "org.netpreserve" % "jwarc" % jwarcVersion,
      "com.softwaremill.sttp.client3" %% "httpclient-backend" % sttpVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion,
      "org.scalameta" %% "munit" % munitVersion % Test
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions += "-Xmx4g",
    Compile / run / javaOptions ++= sparkJavaOptions,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := "app.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(
            "META-INF",
            "org",
            "apache",
            "logging",
            "log4j",
            "core",
            "config",
            "plugins",
            "Log4j2Plugins.dat"
          ) =>
        MergeStrategy.discard
      case PathList("META-INF", fileName)
          if fileName.equalsIgnoreCase("MANIFEST.MF") =>
        MergeStrategy.discard
      case PathList("google", "protobuf", fileName)
          if fileName.endsWith(".proto") =>
        MergeStrategy.first
      case "arrow-git.properties" =>
        MergeStrategy.discard
      case PathList("META-INF", "services", _*) =>
        MergeStrategy.concat
      case PathList("META-INF", fileName)
          if fileName.toLowerCase.endsWith(".sf") ||
            fileName.toLowerCase.endsWith(".dsa") ||
            fileName.toLowerCase.endsWith(".rsa") =>
        MergeStrategy.discard
      case PathList("META-INF", _*) =>
        MergeStrategy.first
      case "module-info.class" =>
        MergeStrategy.discard
      case x =>
        (assembly / assemblyMergeStrategy).value(x)
    }
  )
