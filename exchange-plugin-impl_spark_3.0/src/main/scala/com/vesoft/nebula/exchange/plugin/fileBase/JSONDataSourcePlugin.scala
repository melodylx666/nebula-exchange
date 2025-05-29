package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, FileBaseSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
//TODO 注意这里的FileBaseReader的位置
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: JSONDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin
* Description: 
* @author lx
* @version 1.0   
*/

class JSONDataSourcePlugin extends DataSourcePlugin {

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "json"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry,fields:List[String]): Option[DataFrame] = {
    val jsonConfig: FileBaseSourceConfigEntry = config.asInstanceOf[FileBaseSourceConfigEntry]
    val reader = new JSONReader(session, jsonConfig)
    //TODO remove the println and use the logging framework
    println("----------JSONDataSourcePlugin begin load Data-----------")
    Some(reader.read())
  }
}


