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
    //这里使用的是当前线程的context loader
    //此方法在master节点主线程执行，所以是后续的所有线程共用一份插件
    val loader: ServiceLoader[DataSourcePlugin] = ServiceLoader.load(classOf[DataSourcePlugin])
    //此处loader是iterable，并且在遍历的时候，插件jar包就会被加载
    loader.forEach(plugin => {
      plugins.put(plugin.category.toLowerCase, plugin)
      LOG.info(s">>>>> Loading and initializing data source plugin: ${plugin.category} successfully!")
    })
  }

  def get(category: String):Option[DataSourcePlugin] = {
    LOG.info(s">>>>> Getting data source plugin:${category}")
    plugins.get(category.toLowerCase)

  }


}
