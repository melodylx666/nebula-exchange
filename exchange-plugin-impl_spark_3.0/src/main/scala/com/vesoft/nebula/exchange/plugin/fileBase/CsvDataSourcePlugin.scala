package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, FileBaseSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
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

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "csv"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry,fields:List[String]): Option[DataFrame] = {
    val csvConfig: FileBaseSourceConfigEntry = config.asInstanceOf[FileBaseSourceConfigEntry]
    val reader = new CSVReader(session, csvConfig)
    //TODO remove the println and use logging framework
    println("----------CSVDataSourcePlugin begin load Data-----------")
    Some(reader.read())
  }
}


