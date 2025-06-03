package com.vesoft.exchange.common.plugin

import com.typesafe.config.Config
import com.vesoft.exchange.common.config.{DataSourceConfigEntry, SourceCategory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 数据源插件接口
 */

trait DataSourcePlugin {

  //categoryT info
  def categoryType:SourceCategory.Value = SourceCategory.CUSTOM
  def categoryName:String

  //configEntity parser
  def dataSourceConfigParser(category: SourceCategory.Value,
                             config: Config,
                             nebulaConfig: Config,
                             variable: Boolean,
                             paths: Map[String, String]): DataSourceConfigEntry

  //load data
  def readData(session:SparkSession,config:DataSourceConfigEntry,fields:List[String]):Option[DataFrame]

}
