package com.vesoft.nebula.exchange.plugin.serverBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, PostgreSQLSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: PgDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin.serverBase
* Description:
 *
 * @author lx
* @version 1.0   
*/
class PgDataSourcePlugin extends DataSourcePlugin{

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "postgresql"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @param fields  ：字段列表
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry, fields: List[String]): Option[DataFrame] = {
    val postgreConfig = config.asInstanceOf[PostgreSQLSourceConfigEntry]
    val reader = new PostgreSQLReader(session, postgreConfig)
    Some(reader.read())
  }
}
