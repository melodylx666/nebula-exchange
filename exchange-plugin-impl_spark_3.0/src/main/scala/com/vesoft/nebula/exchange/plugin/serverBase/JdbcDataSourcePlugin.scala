package com.vesoft.nebula.exchange.plugin.serverBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, JdbcConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: JdbcDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin.serverBase
* Description:
 *
 * @author lx
* @version 1.0   
*/
class JdbcDataSourcePlugin extends DataSourcePlugin{

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "jdbc"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   * //TODO 其实可以直接将reader冗余到createReader函数中。具体需要考虑多个module之间的reader类可不可以复用
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @param fields  ：字段列表
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry, fields: List[String]): Option[DataFrame] = {
    val jdbcConfig = config.asInstanceOf[JdbcConfigEntry]
    val reader     = new JdbcReader(session, jdbcConfig)
    Some(reader.read())
  }
}
