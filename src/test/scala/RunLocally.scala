
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.test.task.titlingpoint.{AppConfig, ETLConf, Main}

object RunLocally {

  def main(args: Array[String]): Unit = {
    val baseTestPath = "/tmp/titlingPoint/"
    val mockedJsonDataPath = s"$baseTestPath/input_data"
    val deltaTablePath = s"$baseTestPath/delta_table"

    // prepare test env.
    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    FileSystem.getLocal(spark.sparkContext.hadoopConfiguration).delete(new Path(baseTestPath), true)

    // prepare data
    val mockedData = spark.read.option("inferSchema", "true").json("src/test/resources/input-data.json")
    new FileStreamSink(spark, mockedJsonDataPath, new JsonFileFormat(), Seq.empty, Map.empty)
      .addBatch(0, mockedData)


    val config = AppConfig(
      inputDataPath = mockedJsonDataPath,
      outputTablePath = deltaTablePath,
      checkpointLocationPath = s"$baseTestPath/checkpoint",
      recordsInFile = 2000000,
      etlConf = ETLConf(inputSparkSchemaInJson = mockedData.schema.prettyJson,
        timestampColumn = "timestamp",
        orderColumn = None, datePartitionColumn = "date",
        hourPartitionColumn = "hour",
        mappingExpression = List("user", "evtname")))
    val stream = Main.runETLStream(config)

    stream.processAllAvailable()

    spark.read.format("delta").load(deltaTablePath).show()
  }

}