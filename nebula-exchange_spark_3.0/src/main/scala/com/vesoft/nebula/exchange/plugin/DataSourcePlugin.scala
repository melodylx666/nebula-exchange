package com.vesoft.nebula.exchange.plugin

import com.vesoft.exchange.common.config.DataSourceConfigEntry
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* 数据源插件接口
 * TODO plugin 类拆分到单独模块中，否则每个插件的custom都需要引入全量依赖。
*/

trait DataSourcePlugin {
  /**
   * 数据源插件类型,比如 mysql,CSV等
   * @return 数据源插件类型字符串
   */
  def category:String

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   * @param session：spark session
   * @param config：数据源配置
   * @return
   */
  def createReader(session:SparkSession,config:DataSourceConfigEntry):Option[DataFrame]

}
