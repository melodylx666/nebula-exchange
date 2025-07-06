package com.vesoft.nebula.exchange.plugin.fileBase

import com.typesafe.config.Config
import com.vesoft.exchange.common.config.{DataSourceConfigEntry, SourceCategory}
import com.vesoft.exchange.common.plugin.DataSourceConfigResolver

/**
*  impl for csv data source
*/
object ConfigResolverImpl extends DataSourceConfigResolver{
  override def getDataSourceConfigEntry(category: SourceCategory.Value, config: Config, nebulaConfig: Config): DataSourceConfigEntry = {
    super.getDataSourceConfigEntry(category, config, nebulaConfig)
  }
}
