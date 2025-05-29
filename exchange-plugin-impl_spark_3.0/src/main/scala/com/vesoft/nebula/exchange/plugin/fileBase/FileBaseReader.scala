package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.FileBaseSourceConfigEntry
import com.vesoft.exchange.common.utils.NebulaUtils.DEFAULT_EMPTY_VALUE
import com.vesoft.nebula.exchange.plugin.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
* ClassName: FileBaseReader
* Package: com.vesoft.nebula.exchange.reader_tmp
* Description: TODO 此类看是否能够放到common类中。
 *
* @author lx
* @version 1.0   
*/
abstract class FileBaseReader(val session: SparkSession, val path: String) extends Reader {

  require(path.trim.nonEmpty)
  override def close(): Unit = {
    session.close()
  }
}

class CSVReader(override val session: SparkSession, csvConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, csvConfig.path) {

  override def read(): DataFrame = {
    session.read
      .option("delimiter", csvConfig.separator.get)
      .option("header", csvConfig.header.get)
      .option("emptyValue", DEFAULT_EMPTY_VALUE)
      .csv(path)
  }
}

class JSONReader(override val session: SparkSession, jsonConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, jsonConfig.path) {

  override def read(): DataFrame = {
    session.read.json(path)
  }
}

class OrcReader(override val session: SparkSession, orcConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, orcConfig.path) {

  override def read(): DataFrame = {
    session.read.orc(path)
  }
}

class ParquetReader(override val session: SparkSession, parquetConfig: FileBaseSourceConfigEntry)
  extends FileBaseReader(session, parquetConfig.path) {

  override def read(): DataFrame = {
    session.read.parquet(path)
  }
}



