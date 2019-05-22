package dataengineering

import dataengineering.utils.{FileReadSettings, Spark}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSuite}

@DoNotDiscover
class LoadTransactionTest extends FunSuite with SparkMocker with ExecutionContext with BeforeAndAfterAll {

  implicit var sparkSession: SparkSession = _

  var txnDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkSession = getSparkSession
  }

  override def afterAll(): Unit = {
    txnDF.repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(txnOutDir)
    stopSparkSession(sparkSession)

    super.afterAll()
  }

  test("Load transaction zip files to DataFrame") {
    val txn1Zip: ZipFileLoader = new ZipFileLoader(txn1File, tempDir.getCanonicalPath)
    val txn1DF: DataFrame = txn1Zip.getDF(FileReadSettings(HEADER = true, SCHEMA = Some(fileSchema)))
    assert(txn1DF.count() === txn1Count)

    val txn2Zip: ZipFileLoader = new ZipFileLoader(txn2File, tempDir.getCanonicalPath)
    val txn2DF: DataFrame = txn2Zip.getDF(FileReadSettings(HEADER = true, SCHEMA = Some(fileSchema)))
    assert(txn2DF.count() === txn2Count)

    txnDF = txn1DF.unionByName(txn2DF)
  }

  test("Check count of transaction DataFrame") {
    assert(txnDF.count() === txn1Count + txn2Count)
  }

  test("Sanitize DataFrame by removing records with vendor prefix") {
    txnDF = Spark.filterPrefixCol(txnDF, col("credit_card_number"), vendorPrefix.values.flatten.toList)
    assert(txnDF.count() === sanitizedTxnCount)
  }
}
