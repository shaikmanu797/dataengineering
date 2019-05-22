package dataengineering.utils

import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.types.StructType

/**
  * `FileReadSettings` provides the read options to be applied on reading delimited files
  *
  * @param HEADER        a boolean to determine if the file has header record on the first line
  * @param DELIMITER     a string character that separates field values
  * @param SCHEMA        an optional StructType to be applied on the DataFrameReader during delimited file read operation
  * @param PARSE_MODE    DataFrameReader csv parse modes to handle the data parsing
  * @param EXTRA_OPTIONS other DataFrameReader csv options that can used on read
  */
final case class FileReadSettings(
                                   HEADER: Boolean = false,
                                   DELIMITER: String = ",",
                                   SCHEMA: Option[StructType] = None,
                                   PARSE_MODE: ParseMode = ParseMode.fromString("PERMISSIVE"),
                                   EXTRA_OPTIONS: Map[String, String] = Map.empty
                                 )
