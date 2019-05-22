package dataengineering

import dataengineering.utils.{FileReadSettings, Spark}
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object Driver extends App {

  implicit val sparkSession: SparkSession = SparkSession.builder().config(Spark.config).enableHiveSupport().getOrCreate()

  val tempDir: String = System.getProperty("java.io.tmpdir")

  val fraudZip: ZipFileLoader = new ZipFileLoader(this.getClass.getResource("/datasets/fraud.zip").getPath, tempDir, false)

  val fraudDF: DataFrame = fraudZip.getDF(FileReadSettings(HEADER = true, INFER_SCHEMA = true, MODE = ParseMode.fromString("DROPMALFORMED")))

  fraudDF.show()

  sparkSession.stop()
}

