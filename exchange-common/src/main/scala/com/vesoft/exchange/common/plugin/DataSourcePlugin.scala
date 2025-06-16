package com.vesoft.exchange.common.plugin

import com.typesafe.config.Config
import com.vesoft.exchange.common.config.{DataSourceConfigEntry, SourceCategory}

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}
/**
 * 数据源插件接口
 */

abstract class DataSourcePlugin {

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

abstract class DataSourcePluginCompanion {
  private val LOG = Logger.getLogger(this.getClass)
  private var _pluginInstance:Option[DataSourcePlugin] = None
  /*-------------------实例管理部分-----------------------------*/

  def createPlugin(name: String):DataSourcePlugin

  final def initPlugin(name: String):Unit = {
    if(_pluginInstance.isEmpty){
      _pluginInstance = Some(createPlugin(name))
    }
  }

  final def getPlugin(name:String):Option[DataSourcePlugin] = {
    _pluginInstance
  }

  final def clearPlugin(): Unit = {
    LOG.info(s">>>>> Clearing plugin instance")
    _pluginInstance = None
  }

}



object DataSourcePlugin{
  private[this] val LOG = Logger.getLogger(this.getClass)
  /*-------------------Companion管理部分-----------------------------*/
  //映射
  private val nameToCompanion = mutable.Map[String,DataSourcePluginCompanion]()
  private val elementToCompanion = mutable.Map[String,DataSourcePluginCompanion]()

  //Companion反射
  private[this] def lookupCompanion(name: String):DataSourcePluginCompanion = {
    Class.forName(name)
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val companionSymbol = mirror.staticModule(name)
    val companion = mirror.reflectModule(companionSymbol).instance.asInstanceOf[DataSourcePluginCompanion]
    companion
  }

  /*-----------------需求转发部分----------------------------------*/
  //配置处理
  def HandleConfig(category: SourceCategory.Value,
                   config: Config,
                   nebulaConfig: Config,
                   variable: Boolean,
                   paths: Map[String, String]):DataSourceConfigEntry = {
      val name = config.getString("type.source")
      val elementName = config.getString("name")
      nameToCompanion.get(name) match {
        case Some(companion) => {
          LOG.info(s">>>>> parser config with ${name} that are already loaded")
          elementToCompanion += elementName -> companion
          companion.getPlugin(name).get.dataSourceConfigParser(category, config, nebulaConfig, variable, paths)
        }
        case None => {
          //初次初始化
          val companion = lookupCompanion(name)
          nameToCompanion += name -> companion
          elementToCompanion += elementName -> companion
          companion.initPlugin(name)
          LOG.info(s">>>>> parser config with ${name} that first loaded")
          companion.getPlugin(name).get.dataSourceConfigParser(category, config, nebulaConfig, variable, paths)
      }
    }
  }

  //读取数据，这里映射关系是并发读，线程安全
  def ReadData(session:SparkSession,config:DataSourceConfigEntry,fields:List[String],element:String):Option[DataFrame] = {
    elementToCompanion.get(element) match {
      case Some(companion) => {
        companion.getPlugin(element).get.readData(session,config,fields)
      }
      case None => {
        LOG.error(s">>>>> element $element not found")
        None
      }
    }
  }
}






