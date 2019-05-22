package dataengineering

import dataengineering.utils.{FileReadSettings, Spark}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class ZipFileLoader(inputFile: String, unzipDir: String, retainExtractedFile: Boolean = false)(implicit sparkSession: SparkSession) extends Logging {

  private val unzipFile: Path = new Path(unzipDir.concat(inputFile.split("/").last.replaceAll(".zip", "")))
  private val fs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
  private val dfr: DataFrameReader = sparkSession.read

  def getDF(options: FileReadSettings = FileReadSettings()): DataFrame = {
    fs.delete(unzipFile, true)
    Spark.zipToRDD(inputFile).saveAsTextFile(unzipFile.toString)
    if(!retainExtractedFile) {
      log.warn("#"*10 + s" Deleting $unzipFile file on JVM exit")
      fs.deleteOnExit(unzipFile)
    }
    dfr.option("header", options.HEADER)
    dfr.option("delimiter", options.DELIMITER)
    if(options.SCHEMA.isEmpty) {
      dfr.option("inferSchema", "true")
    }else {
      log.info("#"*10 + s" Applying provided schema ${options.SCHEMA.get.treeString}")
      dfr.option("inferSchema", "false")
      dfr.schema(options.SCHEMA.get)
    }
    dfr.option("mode", options.PARSE_MODE.name)
    dfr.options(options.EXTRA_OPTIONS)
    dfr.csv(unzipFile.toString).na.drop("all")
  }
}