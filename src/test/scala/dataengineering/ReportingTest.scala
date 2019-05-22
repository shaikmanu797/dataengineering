package dataengineering

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import dataengineering.utils.Spark
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSuite}

@DoNotDiscover
class ReportingTest extends FunSuite with SparkMocker with DataFrameSuiteBase with ExecutionContext with BeforeAndAfterAll {
  implicit var sparkSession: SparkSession = _
  var fraudDF: DataFrame = _
  var txnDF: DataFrame = _
  var expectedDF: DataFrame = _
  var reporting: Reporting = _
  var byteLength: UserDefinedFunction = udf((s: String) => s.getBytes("UTF-8").length)

  override def appID: String = this.getClass.getName + math.floor(math.random * 10E4).toLong.toString

  override def enableHiveSupport: Boolean = true

  override def beforeAll(): Unit = {
    sparkSession = getSparkSession

    fraudDF = JDBC.read(jdbcProps("user"), jdbcProps("password"), new JDBCOptions(jdbcProps))
    txnDF = sparkSession.read.option("header", "true").schema(fileSchema).csv(txnOutDir)
    reporting = new Reporting(fraudDF, txnDF)

    expectedDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row(30340825930914L, "192.168.222.52", "TX"),
        Row(562632689286L, "10.198.220.41", "CT"),
        Row(675979363636L, "172.21.115.184", "MT"),
        Row(675943613025L, "192.168.110.0", "RI")
      )),
      fileSchema
    )
  }

  override def afterAll(): Unit = {
    stopSparkSession(sparkSession)
  }

  test("Load Fraud data from MySql table") {
    fraudDF.cache()
    assert(fraudDF.count() === fraudCount)
  }

  test("Load sanitized Transaction file from filesystem") {
    txnDF.cache()
    assert(txnDF.count() === sanitizedTxnCount)
  }

  test("Reporting fraudulent transactions per state match") {
    assertDataFrameEquals(expectedDF, reporting.findFraudTxn)
    val expectedFraudTxnAgg: DataFrame = expectedDF.groupBy(col("state")).agg(count(lit(1)).alias("Num Of Fraudulent Transactions"))
    val actualFraudTxnAgg: DataFrame = reporting.countFraudTxnByCol(col("state"))
    assertDataFrameEquals(expectedFraudTxnAgg, actualFraudTxnAgg)
    Spark.saveAsJson(actualFraudTxnAgg.repartition(1), fraudPerStateReport)
    assert(new File(fraudPerStateReport).listFiles().exists(_.getName.endsWith(".json")))
  }

  test("Creating masked dataset and reporting in JSON") {
    val maskedDF: DataFrame = txnDF.withColumn("credit_card_number_masked", regexp_replace(col("credit_card_number"), "\\d{9}$", "*" * 9))
      .withColumn("sum_num_bytes", byteLength(concat(col("credit_card_number").cast("string"), col("ipv4"), col("state"))))
      .select("credit_card_number_masked", "ipv4", "state", "sum_num_bytes")
      .repartition(1)
    Spark.saveAsJson(maskedDF, maskedDatasetReport)
    assert(new File(maskedDatasetReport).listFiles().exists(_.getName.endsWith(".json")))
  }
}
