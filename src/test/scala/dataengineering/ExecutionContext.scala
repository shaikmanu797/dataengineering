package dataengineering

import java.io.File
import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType

trait ExecutionContext {
  val jdbcProps: Map[String, String] = Map(
    JDBCOptions.JDBC_URL -> "jdbc:mysql://localhost:3306",
    JDBCOptions.JDBC_TABLE_NAME -> "test.fraud",
    JDBCOptions.JDBC_NUM_PARTITIONS -> "2",
    JDBCOptions.JDBC_DRIVER_CLASS -> "com.mysql.jdbc.Driver",
    "user" -> "root",
    "password" -> ""
  )

  val fraudFile: String = this.getClass.getResource("/datasets/fraud.zip").getPath
  val txn1File: String = this.getClass.getResource("/datasets/transaction-001.zip").getPath
  val txn2File: String = this.getClass.getResource("/datasets/transaction-002.zip").getPath
  val fileSchema: StructType = StructType.fromDDL("credit_card_number long, ipv4 string, state string")

  val txnOutDir: String = new File(dataDir).getCanonicalPath.concat("/txn")
  val reportDir: String = new File(dataDir).getCanonicalPath.concat("/reports")
  val fraudPerStateReport: String = reportDir.concat("/fraud_txn_per_state")
  val maskedDatasetReport: String = reportDir.concat("/masked_dataset")

  private def dataDir: String = Option(System.getenv("DATA_DIR")).getOrElse(System.getProperty("java.io.tmpdir"))

  val vendorPrefix: Map[String, List[String]] = Map(
    "maestro" -> List("5018", "5020", "5038", "56##"),
    "mastercard" -> List("51", "52", "54", "55", "222%"),
    "visa" -> List("4"),
    "amex" -> List("34", "37"),
    "discover" -> List("6011", "65"),
    "diners" -> List("300", "301", "304", "305", "36", "38"),
    "jcb16" -> List("35"),
    "jcb15" -> List("2131", "1800")
  )

  val fraudCount: Long = 1011
  val txn1Count: Long = 250000
  val txn2Count: Long = 250000
  val sanitizedTxnCount: Long = 56471

  def getConnection(jdbcOpts: JDBCOptions = new JDBCOptions(jdbcProps)): Connection = DriverManager.getConnection(jdbcOpts.url, jdbcOpts.asProperties)

  def createDatabase(conn: Connection, db: String = "test"): Unit = {
    val stmt: Statement = conn.createStatement()
    stmt.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $db;")
    stmt.close()
  }

  def destroyDatabase(conn: Connection, db: String = "test"): Unit = {
    val stmt: Statement = conn.createStatement()
    stmt.executeUpdate(s"DROP DATABASE IF EXISTS $db;")
    stmt.close()
  }
}
