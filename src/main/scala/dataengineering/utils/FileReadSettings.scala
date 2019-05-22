package dataengineering.utils

import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.types.StructType

final case class FileReadSettings(
                                   HEADER: Boolean = false,
                                   DELIMITER: String = ",",
                                   SCHEMA: Option[StructType] = None,
                                   PARSE_MODE: ParseMode = ParseMode.fromString("PERMISSIVE"),
                                   EXTRA_OPTIONS: Map[String, String] = Map.empty
                                 )
