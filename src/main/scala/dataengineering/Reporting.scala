package dataengineering

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Factory for BI reporting
  *
  * @param fraudDF      a DataFrame instance holding records of the fraud transactions provided
  * @param txnDF        a DataFrame instance holding records of the sanitized transactions after removing vendor prefix credit_card_num records
  * @param sparkSession current SparkSession instance
  */
class Reporting(fraudDF: DataFrame, txnDF: DataFrame)(implicit sparkSession: SparkSession) extends Logging {
  /**
    * `countFraudTxnByCol` performs aggregation `count` on DataFrame based on specified column
    *
    * @param col column on the DataFrame
    * @param df  a DataFrame instance to perform aggregation
    * @return an aggregated DataFrame
    */
  def countFraudTxnByCol(col: Column, df: DataFrame = findFraudTxn): DataFrame = {
    findFraudTxn.groupBy(col).agg(count(lit(1)).alias("Num Of Fraudulent Transactions"))
  }

  /**
    * `findFraudTxn` finds the fraudulent transactions by intersecting the sanitized transactions DataFrame with provided fraud transactions DataFrame
    *
    * @return a DataFrame holding records of fraudulent transactions
    */
  def findFraudTxn: DataFrame = txnDF.intersect(fraudDF)
}
