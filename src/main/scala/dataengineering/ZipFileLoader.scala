package dataengineering

import dataengineering.utils.{FileReadSettings, Spark}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

/**
  * Factory for loading zip files
  *
  * @param inputFile           Absolute path to the .zip file
  * @param unzipDir            an intermediate directory / temporary directory to save the uncompressed file
  * @param retainExtractedFile a boolean flag to determine if the uncompressed file has to be deleted on JVM exit
  * @param sparkSession        current SparkSession instance
  */
class ZipFileLoader(inputFile: String, unzipDir: String, retainExtractedFile: Boolean = false)(implicit sparkSession: SparkSession) extends Logging {

  private val unzipFile: Path = new Path(unzipDir.concat(inputFile.split("/").last.replaceAll(".zip", "")))
  private val fs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
  private val dfr: DataFrameReader = sparkSession.read

  /**
    * `getDF` applies provided csv options on DataFrameReader while reading the uncompressed delimited file
    *
    * @param options `FileReadSettings` instance that holds options to be applied on the compressed file within zip
    * @return a DataFrame
    */
  def getDF(options: FileReadSettings = FileReadSettings()): DataFrame = {
    fs.delete(unzipFile, true)
    Spark.zipToRDD(inputFile).saveAsTextFile(unzipFile.toString)
    if (!retainExtractedFile) {
      log.warn("#" * 10 + s" $unzipFile file set to delete on JVM exit")
      fs.deleteOnExit(unzipFile)
    }
    dfr.option("header", options.HEADER)
    dfr.option("delimiter", options.DELIMITER)
    if (options.SCHEMA.isEmpty) {
      dfr.option("inferSchema", "true")
    } else {
      log.info("#" * 10 + s" Applying provided schema\n${options.SCHEMA.get.treeString}")
      dfr.option("inferSchema", "false")
      dfr.schema(options.SCHEMA.get)
    }
    dfr.option("mode", options.PARSE_MODE.name)
    dfr.options(options.EXTRA_OPTIONS)
    dfr.csv(unzipFile.toString).na.drop("all")
  }
}