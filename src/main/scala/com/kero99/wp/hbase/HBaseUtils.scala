package com.kero99.wp.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  *
  * @author wp
  * @date 2019-08-30 14:35
  *
  */
object HBaseUtils {
  private val cfg = HBaseConfiguration.create()
  cfg.set("hbase.zookeeper.quorum","apache01:2181,apache02:2181,apache03:2181")
  private val conn = ConnectionFactory.createConnection(cfg)
  private val admin= conn.getAdmin

  def getCfg()=cfg
  def getConn()=conn
  def getAdmin()=admin
  def getTable(tname:String) = conn.getTable(TableName.valueOf(tname))

}
