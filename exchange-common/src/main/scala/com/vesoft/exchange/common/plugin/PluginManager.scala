package com.vesoft.exchange.common.plugin

/**
 * 插件管理类
 */
import org.apache.log4j.Logger

import java.util.ServiceLoader
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

object PluginManager {
  private[this] val LOG = Logger.getLogger(this.getClass)
  // full name to plugin
  private  val plugins = mutable.Map[String,DataSourcePlugin]()
  // tag/edge to plugin
  private  val elemToSinglePlugin = mutable.Map[String,DataSourcePlugin]()

  def init():Unit = {
    //这里使用的是当前线程的context loader，这里是application class Loader
    //此方法在master节点主线程执行，所以是后续的所有线程共用一份插件
    //如果未初始化，就加载插件。否则直接跳过。
    try{
      if(plugins.isEmpty){
        val loader: ServiceLoader[DataSourcePlugin] = ServiceLoader.load(classOf[DataSourcePlugin])
        loader.forEach(plugin => {
          plugins.put(plugin.getClass.getName, plugin)
          LOG.info(s">>>>> Loading and initializing data source plugin: ${plugin.getClass.getName} successfully!")
        })
      }
    } catch {
      case e: Exception =>
        LOG.error(s">>>>> Failed to load data source plugins: ${e.getMessage}")
    }
  }

  def get(name: String):Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting data source plugin:${name}")
    plugins.get(name)
  }

  def get():Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting the first data source plugin")
    plugins.values.headOption
  }

  //  tag/edge To plugin class
  def elemToPlugin(name: String, plugin: DataSourcePlugin):Unit = {
    LOG.info(s">>>>> Binding the Tag/Edge: ${name} with plugin:${plugin.getClass.getName}")
    elemToSinglePlugin.put(name, plugin)
  }

  def getPluginByElem(name: String):Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting data source plugin by Tag/Edge:${name}")
    elemToSinglePlugin.get(name)
  }

  private def clear():Unit = {
    LOG.info(s">>>>> Clearing data source plugins memo cache")
    plugins.clear()
  }


}
