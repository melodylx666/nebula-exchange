package com.vesoft.nebula.exchange.plugin.fileBase

import com.typesafe.config.Config
import com.vesoft.exchange.common.config.{DataSourceConfigEntry, FileBaseSourceConfigEntry, SourceCategory}
import com.vesoft.exchange.common.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: CsvDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin
* Description: nebulaGraph - spark3.x - csv数据源插件
 *  TODO  区分不同Base，因该拆分到不同的包中
* @author lx
* @version 1.0   
*/

class CsvDataSourcePlugin extends DataSourcePlugin {
  override def categoryName: String = "csv-custom"

  override def dataSourceConfigParser(category: SourceCategory.Value, config: Config, nebulaConfig: Config, variable: Boolean, paths: Map[String, String]): DataSourceConfigEntry = {
    //copy from default mode data source for test
    val separator =
      if (config.hasPath("separator"))
        config.getString("separator")
      else ","
    val header =
      if (config.hasPath("header"))
        config.getBoolean("header")
      else
        false
    if (variable)
      FileBaseSourceConfigEntry(SourceCategory.CUSTOM,
        paths(config.getString("path")),
        Some(separator),
        Some(header))
    else
      FileBaseSourceConfigEntry(SourceCategory.CUSTOM,
        config.getString("path"),
        Some(separator),
        Some(header))
  }

  override def readData(session: SparkSession, config: DataSourceConfigEntry, fields: List[String]): Option[DataFrame] = {
    val csvConfig = config.asInstanceOf[FileBaseSourceConfigEntry]
    val reader =
      new CSVReader(session, csvConfig)
    Some(reader.read())
  }
}


