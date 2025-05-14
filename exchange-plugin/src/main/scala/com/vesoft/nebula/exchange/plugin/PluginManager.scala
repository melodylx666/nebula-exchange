package com.vesoft.nebula.exchange.plugin

/**
 * 插件管理类
 */
import java.util.ServiceLoader
import scala.collection.mutable
import org.apache.log4j.Logger

object PluginManager {
  private[this] val LOG = Logger.getLogger(this.getClass)
  private  val plugins = mutable.Map[String,DataSourcePlugin]()

  def init():Unit = {
    val loader: ServiceLoader[DataSourcePlugin] = ServiceLoader.load(classOf[DataSourcePlugin])
    loader.forEach(plugin => {
      plugins.put(plugin.category.toLowerCase, plugin)
      LOG.info(s">>>>> Loaded data source plugin: ${plugin.category}")
    })
  }

  def get(category: String):Option[DataSourcePlugin] = {
    plugins.get(category.toLowerCase)
  }


}
