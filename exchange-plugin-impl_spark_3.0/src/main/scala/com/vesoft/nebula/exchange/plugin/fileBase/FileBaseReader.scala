package com.vesoft.nebula.exchange.plugin.fileBase

import com.vesoft.exchange.common.config.{CustomSourceConfigEntry, FileBaseSourceConfigEntry}
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

class CSVReader(override val session: SparkSession, csvConfig: CustomSourceConfigEntry)
  extends FileBaseReader(session, csvConfig.rawConfig.getString("path")) {

  override def read(): DataFrame = {
    var separator = ","
    var header = false
    if(csvConfig.rawConfig.hasPath("custom")){
      val customConfig = csvConfig.rawConfig.getConfig("custom")
      separator = customConfig.getString("separator")
      header = customConfig.getBoolean("header")
      println(s">>>>> Here ${separator}-------------${header}")
    }
    session.read
      .option("delimiter", separator)
      .option("header", header)
      .option("emptyValue", DEFAULT_EMPTY_VALUE)
      .csv(path)
  }
}




