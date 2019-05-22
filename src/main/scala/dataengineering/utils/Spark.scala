package dataengineering.utils

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Spark extends Logging {
  def config: SparkConf = {
    val defaultConf: SparkConf = new SparkConf()
    defaultConf.setIfMissing("spark.master", "local[4]")
    defaultConf.setIfMissing("spark.app.name", "dataengineering")
    defaultConf.setIfMissing("spark.ui.enabled", "false")
    defaultConf.setIfMissing("spark.launcher.childProcLoggerName", "dataengineering")
    defaultConf.setIfMissing("hive.exec.dynamic.partition", "true")
    defaultConf.setIfMissing("hive.exec.dynamic.partition.mode", "nonstrict")
    defaultConf
  }

  def zipToRDD(zipFile: String)(implicit sparkSession: SparkSession): RDD[String] = {
    log.info("#"*10 + s" Reading compressed file inside $zipFile to a RDD of class String")
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
}
