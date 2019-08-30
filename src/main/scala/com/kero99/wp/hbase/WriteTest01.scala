package com.kero99.wp.hbase

import java.util

import com.kero99.wp.WpSession
import org.apache.hadoop.hbase.client.Put
/**
  *
  * @author wp
  * @date 2019-08-30 15:38
  *
  */
object WriteTest01 extends WpSession{
  val htable=HBaseUtils.getTable("tb_users")
  val list = new util.ArrayList[Put]()

  for(i <- 1 to 10){
    val put = new Put(s"spark${i+10}".getBytes)
    put.addColumn("info".getBytes,"name".getBytes,s"杨子轩${i}".getBytes)
    put.addColumn("info".getBytes,"age".getBytes,s"${i+18}".getBytes)
    put.addColumn("info".getBytes,"sex".getBytes,s"${if(i%2==0) "nan" else "nv"}".getBytes)
    list.add(put)
  }
  htable.put(list)

}
