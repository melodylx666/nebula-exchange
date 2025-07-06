package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.{CustomSourceConfigEntry, DataSourceConfigEntry}
import com.vesoft.exchange.common.plugin.DataSourceCustomReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
*
*/
object CustomReaderImpl extends DataSourceCustomReader {
  override def readData(session: SparkSession, config: DataSourceConfigEntry, fields: List[String]): Option[DataFrame] = {
    val csvConfig = config.asInstanceOf[CustomSourceConfigEntry]
    val reader = new CSVReader(session, csvConfig)
    Some(reader.read())
  }

}
