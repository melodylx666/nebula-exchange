package com.vesoft.nebula.exchange.plugin.serverBase

import com.vesoft.exchange.common.config.{ClickHouseConfigEntry, DataSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: CkDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin.serverBase
* Description:
 *
 * @author lx
* @version 1.0   
*/
class CkDataSourcePlugin extends DataSourcePlugin{

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "clickhouse"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry,fields:List[String]): Option[DataFrame] = {
    val clickhouseConfigEntry = config.asInstanceOf[ClickHouseConfigEntry]
    val reader                = new ClickhouseReader(session, clickhouseConfigEntry)
    Some(reader.read())
  }
}
