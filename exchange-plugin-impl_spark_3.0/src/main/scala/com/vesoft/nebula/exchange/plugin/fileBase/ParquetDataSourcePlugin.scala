package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, FileBaseSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: ParquetDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin
* Description: 
* @author lx
* @version 1.0   
*/class ParquetDataSourcePlugin extends DataSourcePlugin {

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "parquet"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry,fields:List[String]): Option[DataFrame] = {
    val parquetConfig: FileBaseSourceConfigEntry = config.asInstanceOf[FileBaseSourceConfigEntry]
    val reader = new ParquetReader(session, parquetConfig)
    //TODO remove the println and use the logging framework
    println("----------ParquetDataSourcePlugin begin load Data-----------")
    Some(reader.read())
  }
}


