//> using scala 3.8.3
//> using options -Yexplicit-nulls
//> using dep org.apache.spark:spark-sql_2.13:3.5.1
//> using dep com.netflix.wick::wick:0.0.4
//> using exclude org.apache.spark:spark-sql_2.13

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parseArgs(args) match {
      case Left(message) =>
        System.err.println(message)
        System.exit(1)

      case Right(CliCommand.ShowUsage) =>
        println(Cli.usage)

      case Right(CliCommand.Run(config)) =>
        runConfig(config)
    }
  }

  private def runConfig(config: JobConfig): Unit =
    config.pipeline match {
      case PipelineMode.LocalSitemaps =>
        LocalRobotsSitemapsPipeline.run(config)

      case _ =>
        val spark = SparkSessionFactory.create(config)
        spark.sparkContext.setLogLevel("WARN")

        try {
          config.pipeline match {
            case PipelineMode.Robots =>
              CommonCrawlRobotsPipeline.run(spark, config)
            case PipelineMode.Sitemaps =>
              CommonCrawlSitemapsPipeline.run(spark, config)
            case PipelineMode.FilterSitemaps =>
              LocalSitemapsFilterPipeline.run(spark, config)
            case PipelineMode.LocalCountrySitemaps =>
              LocalSitemapsFilterPipeline.runCountry(spark, config)
            case PipelineMode.CountrySitemaps =>
              CommonCrawlCountrySitemapsPipeline.run(spark, config)
            case PipelineMode.DownloadSitemaps =>
              LocalSitemapDownloadPipeline.run(spark, config)
            case PipelineMode.LocalSitemaps =>
              LocalRobotsSitemapsPipeline.run(config)
          }
        } finally {
          spark.stop()
        }
    }
}
