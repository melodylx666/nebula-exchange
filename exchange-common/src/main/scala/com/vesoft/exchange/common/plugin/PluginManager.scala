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
  private  val plugins = mutable.Map[String,DataSourcePlugin]()

  def init():Unit = {
    //这里使用的是当前线程的context loader
    //此方法在master节点主线程执行，所以是后续的所有线程共用一份插件
    //如果未初始化，就加载插件。否则直接跳过。
    if(plugins.isEmpty){
      val loader: ServiceLoader[DataSourcePlugin] = ServiceLoader.load(classOf[DataSourcePlugin])
      //此处loader是iterable，并且在遍历的时候，插件jar包就会被加载
      loader.forEach(plugin => {
        plugins.put(plugin.categoryName.toLowerCase, plugin)
        LOG.info(s">>>>> Loading and initializing data source plugin: ${plugin.categoryName} successfully!")
      })
    }
  }

  def get(category: String):Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting data source plugin:${category}")
    plugins.get(category.toLowerCase)
  }
  def get():Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting the first data source plugin")
    plugins.values.headOption
  }


}
