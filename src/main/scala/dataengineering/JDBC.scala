package dataengineering

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Factory for `JDBC` utilities
  */
object JDBC extends Logging {
  /**
    * `read` reads the table from RDBMS into DataFrame via JDBC
    *
    * @param jdbcUser     an username to connect with the RDBMS via JDBC
    * @param jdbcPassword password for the username to connect with the RDBMS via JDBC
    * @param settings     `JDBCOptions` that are required by spark DataFrameReader over jdbc functionality
    * @param sparkSession Current SparkSession instance
    * @return a DataFrame
    */
  def read(jdbcUser: String, jdbcPassword: String, settings: JDBCOptions)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read.jdbc(settings.url, settings.tableOrQuery, getConnPropsWithCreds(jdbcUser, jdbcPassword, settings.asProperties))
  }

  /**
    * `getConnPropsWithCreds` adds username and password to the connection properties
    *
    * @param username  an username to connect with the RDBMS via JDBC
    * @param password  password for the username to connect with the RDBMS via JDBC
    * @param connProps Connection Properties that are required by spark DataFrameReader over jdbc functionality
    * @return Properties
    */
  private[this] def getConnPropsWithCreds(username: String, password: String, connProps: Properties): Properties = {
    connProps.setProperty("user", username)
    connProps.setProperty("password", password)
    connProps
  }

  /**
    * `write` writes the DataFrame into specified table on RDBMS via JDBC
    *
    * @param df           a DataFrame instance
    * @param writeMode    `SaveMode` for the Spark JDBC operation
    * @param jdbcUser     an username to connect with the RDBMS via JDBC
    * @param jdbcPassword password for the username to connect with the RDBMS via JDBC
    * @param settings     `JDBCOptions` that are required by spark DataFrameReader over jdbc functionality
    * @return an Unit
    */
  def write(df: DataFrame, writeMode: SaveMode, jdbcUser: String, jdbcPassword: String, settings: JDBCOptions): Unit = {
    df.repartition(settings.numPartitions.get)
      .write
      .mode(writeMode)
      .jdbc(settings.url, settings.tableOrQuery, getConnPropsWithCreds(jdbcUser, jdbcPassword, settings.asProperties))
  }
}
