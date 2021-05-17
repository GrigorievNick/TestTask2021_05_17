package org.test.task.titlingpoint

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}

// TODO split to source and sink
case class ETLConf(inputSparkSchemaInJson: String,
                   timestampColumn: String, // TODO timestmap pattern configuration.
                   orderColumn: Option[String],
                   datePartitionColumn: String,
                   hourPartitionColumn: String,
                   mappingExpression: List[String])

case class AppConfig(inputDataPath: String,
                     outputTablePath: String,
                     checkpointLocationPath: String,
                     recordsInFile: Long,
                     etlConf: ETLConf)

object Main {

  def main(args: Array[String]): Unit = {
    // TODO log input configs
    // TODO Ficus or Pure config mapping from HOCON
    implicit val sparkSession: SparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate()
    val stream = runETLStream(null)
    stream.awaitTermination() // TODO graceful shutdown
  }

  def runETLStream(conf: AppConfig)(implicit spark: SparkSession): StreamingQuery = {
    val ETLConf(inputSchema, timestampColumn, _, datePartitionColumn, hourPartitionColumn, mappingExpression) =
      conf.etlConf

    // Because test task does not have any information about file structure of data on S3, I make thing very simple to me.
    // Pay attention that previously, this deliver exactly once only if EMRFS used or table format like _spark_metadata or delta.
    // But few month ago AWS announce that s3 is not eventually consistent any more. So this solution may work.
    spark
      .readStream
      .schema(DataType.fromJson(inputSchema).asInstanceOf[StructType]) // TODO in real world scenario must be swiped to normal schema managment
      // TODO configure backpressure options to avoid OOM exception or disk io overhelming
      .json(conf.inputDataPath)
      .select((timestampColumn :: mappingExpression).map(col): _*) // Apply etls
      .withColumn(datePartitionColumn, date_format(col(timestampColumn), "yyyy-MM-dd"))
      .withColumn(hourPartitionColumn, hour(col(timestampColumn)))
      // TODO improve filesize and ordering in spark 3 for non streaming application
      //      .repartitionByRange(datePartitionColumn, hourPartitionColumn, orderColumn, )
      .writeStream
      .option("checkpointLocation", conf.checkpointLocationPath)

      .format("delta") // To simplify schema evolution
      .partitionBy(datePartitionColumn, hourPartitionColumn)

      .start(conf.outputTablePath)

  }
}
