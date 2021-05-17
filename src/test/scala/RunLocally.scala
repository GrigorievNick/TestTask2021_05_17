
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.test.task.titlingpoint.{AppConfig, ETLConf, Main, RevenuSumConf}

object RunLocally {

  def main(args: Array[String]): Unit = {
    val baseTestPath = "/tmp/titlingPoint/"
    val mockedJsonDataPath = s"$baseTestPath/input_data"
    val userBasePath = s"$baseTestPath/user"
    val userTablePath = s"$userBasePath/table"
    val revenueBasePath = s"$baseTestPath/revenue"
    val revenueTablePath = s"$revenueBasePath/table"

    // prepare test env.
    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    FileSystem.getLocal(spark.sparkContext.hadoopConfiguration).delete(new Path(baseTestPath), true)

    // prepare data
    val mockedData = spark.read.option("inferSchema", "true").json("src/test/resources/input-data.json")
    val mockDataStream = new FileStreamSink(spark, mockedJsonDataPath, new JsonFileFormat(), Seq.empty, Map.empty)
    mockDataStream.addBatch(0, mockedData)

    val config = AppConfig(
      inputDataPath = mockedJsonDataPath,
      inputSparkSchemaInJson = mockedData.schema.prettyJson,
      etl = ETLConf(outputTablePath = userTablePath,
        checkpointLocationPath = s"$userBasePath/checkpoint",
        timestampColumn = "timestamp",
        datePartitionColumn = "date",
        hourPartitionColumn = "hour",
        mappingExpression = List("user", "evtname")),
      revenueSum = RevenuSumConf(
        outputTablePath = revenueTablePath,
        checkpointLocationPath = s"$revenueBasePath/checkpoint")
    )
    val user = Main.runETLsStream(config)
    user.processAllAvailable()
    spark.read.format("delta").load(userTablePath).show()

    val revenue = Main.runRevenueAggStream(config)
    revenue.processAllAvailable()
    spark.read.format("delta").load(revenueTablePath).show()
    mockDataStream.addBatch(1, mockedData)
    revenue.processAllAvailable()
    spark.read.format("delta").load(revenueTablePath).show()
  }

}