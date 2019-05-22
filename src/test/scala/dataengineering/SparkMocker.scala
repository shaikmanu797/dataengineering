package dataengineering

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.Suite

trait SparkMocker {
  this: Suite =>

  val tempDir: File = MockerUtils.createTempDir()
  private val localWarehousePath: File = new File(tempDir, "warehouse")
  private val localMetastorePath: File = new File(tempDir, "metastore")

  def getSparkSession: SparkSession = {
    val ssb: SparkSession.Builder = SparkSession.builder().config(sparkConfig())
    if (enableHiveSupport) ssb.enableHiveSupport()
    ssb.getOrCreate()
  }

  def enableHiveSupport: Boolean = true

  def sparkConfig(sparkConf: Option[SparkConf] = None): SparkConf = {
    if (sparkConf.isEmpty) {
      val conf: SparkConf = new SparkConf()
      conf.setAppName("dataengineering_test")
      conf.setMaster("local[2]")
      conf.set("spark.logConf", "true")
      conf.set("spark.app.id", appID)
      conf.set("spark.ui.enabled", "false")
      conf.set(SQLConf.CODEGEN_FALLBACK.key, "false")
      conf.set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=${localMetastorePath.getCanonicalPath};create=true")
      conf.set("datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      conf.set(ConfVars.METASTOREURIS.varname, "")
      conf.set("spark.sql.streaming.checkpointLocation", tempDir.toPath.toString)
      conf.set("spark.sql.warehouse.dir", localWarehousePath.getCanonicalPath)
      conf.set("spark.sql.shuffle.partitions", "1")
      conf.set("spark.sql.parquet.compression.codec", "snappy")
      conf.set("hive.exec.dynamic.partition", "true")
      conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    } else {
      sparkConf.get
    }
  }

  def appID: String = this.getClass.getName + math.floor(math.random * 10E4).toLong.toString

  def stopSparkSession(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  }
}
