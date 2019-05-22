package dataengineering.utils

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

/**
  * Factory for `Spark` utilities
  */
object Spark extends Logging {
  /**
    * `zipToRDD` reads the compressed file using Spark into an RDD
    *
    * @param zipFile      Absolute path to the compressed zip file
    * @param sparkSession Current SparkSession instance
    * @return a RDD of class String
    */
  def zipToRDD(zipFile: String)(implicit sparkSession: SparkSession): RDD[String] = {
    log.info("#" * 10 + s" Reading compressed file inside $zipFile to a RDD of class String")
    sparkSession.sparkContext.binaryFiles(zipFile)
      .flatMap({
        case (filePath: String, content: PortableDataStream) =>
          val zis: ZipInputStream = new ZipInputStream(content.open())
          Stream.continually(zis.getNextEntry)
            .takeWhile(_ != null)
            .flatMap(x => {
              val br: BufferedReader = new BufferedReader(new InputStreamReader(zis))
              Stream.continually(br.readLine()).takeWhile(_ != null)
            })
      })
  }

  /**
    * `filterPrefixCol` discards the records that has provided prefixes on a specific column
    *
    * @param df  a DataFrame instance
    * @param col a column on the DataFrame frame on which the prefixes have to be checked
    * @return a DataFrame with discarded prefix records
    */
  def filterPrefixCol(df: DataFrame, col: Column, prefix: List[String]): DataFrame = {
    val rlikePattern: String = s"^(${prefix.mkString("|")})(.*)+$$"
    df.filter(!col.rlike(rlikePattern))
  }

  /**
    * `saveAsJson` writes the provided DataFrame into the target directory with JSON file format
    *
    * @param outputDF  a DataFrame instance
    * @param outputDir Absolute path to the output directory to which the JSON output files are written
    */
  def saveAsJson(outputDF: DataFrame, outputDir: String): Unit = {
    log.info("#" * 10 + s" Writing DataFrame into $outputDir")
    outputDF.write.mode(SaveMode.Overwrite).json(outputDir)
  }
}
