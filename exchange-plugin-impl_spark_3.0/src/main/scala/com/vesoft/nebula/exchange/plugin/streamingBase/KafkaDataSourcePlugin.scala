package com.vesoft.nebula.exchange.plugin.streamingBase

import com.vesoft.exchange.common.config.{DataSourceConfigEntry, KafkaSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.DataSourcePlugin
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: KafkaDataSourcePlugin
* Package: com.vesoft.nebula.exchange.plugin.streamingBase
* Description:
 *
 * @author lx
* @version 1.0   
*/
class KafkaDataSourcePlugin extends DataSourcePlugin{

  /**
   * 数据源插件类型,比如 mysql,CSV等
   *
   * @return 数据源插件类型字符串
   */
  override def category: String = "kafka"

  /**
   * 创建数据源插件的reader，其将读取并返回DataFrame
   *
   * @param session ：spark session
   * @param config  ：数据源配置
   * @param fields  ：字段
   * @return
   */
  override def createReader(session: SparkSession, config: DataSourceConfigEntry,fields: List[String]): Option[DataFrame] = {
    val kafkaConfig = config.asInstanceOf[KafkaSourceConfigEntry]
    //这里直接穿透过来，但是只有Kafka会用到这个字段
    //TODO 修改结构
    val reader = new KafkaReader(session, kafkaConfig, fields)
    Some(reader.read())
    //TODO fileds穿透不过来
    ???
  }
}
