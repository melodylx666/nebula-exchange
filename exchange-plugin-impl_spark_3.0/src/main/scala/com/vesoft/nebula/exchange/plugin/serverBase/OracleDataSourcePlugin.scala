package com.vesoft.nebula.exchange.plugin.serverBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, OracleConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: OracleDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin.serverBase
* Description:
 *
 * @author lx
* @version 1.0   
*/
class OracleDataSourcePlugin extends DataSourcePlugin{

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "oracle"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @param fields  ：字段列表
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry, fields: List[String]): Option[DataFrame] = {
    val oracleConfig = config.asInstanceOf[OracleConfigEntry]
    val reader       = new OracleReader(session, oracleConfig)
    Some(reader.read())
  }
}
