import java.io.File

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  private val AnalysisPartitions = "12"
  private val ExecutorInstances = "50"
  private val ExecutorMemory = "200m"
  private val ReservedMemory = "0"
  private val DriverScalaLibraryPath =
    Option(
      classOf[scala.collection.immutable.ArraySeq[?]]
        .getProtectionDomain()
        .getCodeSource()
    )
      .map(_.getLocation().toURI().getPath())
      .getOrElse("")
  private val DriverRuntimeClassPath =
    System.getProperty("java.class.path", "")
  private val ExecutorClassPath =
    Seq(DriverScalaLibraryPath, DriverRuntimeClassPath)
      .filter(_.nonEmpty)
      .mkString(File.pathSeparator)
  private val SparkJavaOptions = Seq(
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

  def create(config: JobConfig): SparkSession = {
    val javaOptions = SparkJavaOptions.mkString(" ")
    val builder = SparkSession
      .builder()
      .appName("Common Crawl Robots Downloader")
      .master(config.master)
      .config("spark.sql.shuffle.partitions", AnalysisPartitions)
      .config("spark.default.parallelism", AnalysisPartitions)
      .config("spark.executor.instances", ExecutorInstances)
      .config("spark.executor.memory", ExecutorMemory)
      .config("spark.testing.reservedMemory", ReservedMemory)
      .config("spark.executor.extraClassPath", ExecutorClassPath)
      .config("spark.driver.extraJavaOptions", javaOptions)
      .config("spark.executor.extraJavaOptions", javaOptions)
      .config("spark.executorEnv.SPARK_SCALA_VERSION", "2.13")
      .config("spark.sql.parquet.compression.codec", "gzip")

    if (config.master.startsWith("local")) {
      builder.bindLocalDriver.getOrCreate()
    } else {
      builder.getOrCreate()
    }
  }

  extension (builder: SparkSession.Builder)
    private def bindLocalDriver: SparkSession.Builder =
      builder
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
}
