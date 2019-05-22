package dataengineering

import java.sql.{Connection, ResultSet, Statement}

import dataengineering.utils.FileReadSettings
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FunSuite}

@DoNotDiscover
class LoadFraudTest extends FunSuite with SparkMocker with ExecutionContext with BeforeAndAfterAll {

  implicit var sparkSession: SparkSession = _

  val conn: Connection = getConnection()
  var actualDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    createDatabase(conn)

    sparkSession = getSparkSession
  }

  override def afterAll(): Unit = {
    conn.close()

    stopSparkSession(sparkSession)

    super.afterAll()
  }

  test("Load Fraud Zip file into DataFrame") {
    val fraudZip: ZipFileLoader = new ZipFileLoader(fraudFile, tempDir.getCanonicalPath)
    actualDF = fraudZip.getDF(FileReadSettings(HEADER = true, SCHEMA = Some(fileSchema)))
    assert(fraudCount === actualDF.count())
  }

  test("Write Fraud DataFrame in MySQL table test.fraud") {
    JDBC.write(actualDF, SaveMode.Overwrite, jdbcProps("user"), jdbcProps("password"), new JDBCOptions(jdbcProps))

    var actualCount: Int = 0
    val stmt: Statement = conn.createStatement()
    val rs: ResultSet = stmt.executeQuery("SELECT count(1) FROM test.fraud;")
    while (rs.next()) {
      actualCount = rs.getInt(1)
    }
    rs.close()
    stmt.close()

    assert(fraudCount === actualCount)
  }
}
