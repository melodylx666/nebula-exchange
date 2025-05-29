package com.vesoft.nebula.exchange.plugin.streamingBase

import com.vesoft.exchange.common.config.{KafkaSourceConfigEntry, PulsarSourceConfigEntry}
import com.vesoft.nebula.exchange.plugin.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StringType

/**
* ClassName: StreamingBaseReader
* Package: com.vesoft.nebula.exchange.plugin.streamingBase
* Description:
 *
* @author lx
* @version 1.0   
*/
/**
 * Spark Streaming
 *
 * @param session
 */
abstract class StreamingBaseReader(override val session: SparkSession) extends Reader {

  override def close(): Unit = {
    session.close()
  }
}

/**
 *
 * @param session
 * @param kafkaConfig
 * @param targetFields
 */
class KafkaReader(override val session: SparkSession,
                  kafkaConfig: KafkaSourceConfigEntry,
                  targetFields: List[String])
  extends StreamingBaseReader(session) {

  require(
    kafkaConfig.server.trim.nonEmpty && kafkaConfig.topic.trim.nonEmpty && targetFields.nonEmpty)

  override def read(): DataFrame = {
    import org.apache.spark.sql.functions._
    import session.implicits._
    val fields = targetFields.distinct
    val reader =
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaConfig.server)
        .option("subscribe", kafkaConfig.topic)
        .option("startingOffsets", kafkaConfig.startingOffsets)

    if (kafkaConfig.securityProtocol.isDefined) {
      reader.option("kafka.security.protocol", kafkaConfig.securityProtocol.get)
      reader.option("kafka.sasl.mechanism", kafkaConfig.mechanism.get)
    }
    if (kafkaConfig.kerberos) {
      reader.option("kafka.sasl.kerberos.service.name", kafkaConfig.kerberosServiceName)
    }

    val maxOffsetsPerTrigger = kafkaConfig.maxOffsetsPerTrigger
    if (maxOffsetsPerTrigger.isDefined)
      reader.option("maxOffsetsPerTrigger", maxOffsetsPerTrigger.get)

    reader
      .load()
      .select($"value".cast(StringType))
      .select(json_tuple($"value", fields: _*))
      .toDF(fields: _*)

  }
}

/**
 *
 * @param session
 * @param pulsarConfig
 */
class PulsarReader(override val session: SparkSession, pulsarConfig: PulsarSourceConfigEntry)
  extends StreamingBaseReader(session) {

  override def read(): DataFrame = {
    session.readStream
      .format("pulsar")
      .option("service.url", pulsarConfig.serviceUrl)
      .option("admin.url", pulsarConfig.adminUrl)
      .options(pulsarConfig.options)
      .load()
  }
}
