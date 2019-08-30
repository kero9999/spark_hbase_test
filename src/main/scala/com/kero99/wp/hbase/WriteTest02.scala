package com.kero99.wp.hbase

import com.kero99.wp.WpSession
import com.kero99.wp.hbase.HBaseUtils.cfg
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client.Put
/**
  *
  * @author wp
  * @date 2019-08-30 16:01
  *
  */
object WriteTest02 extends WpSession{
  val jobCfg = new JobConf()
  jobCfg.set("hbase.zookeeper.quorum","apache01:2181,apache02:2181,apache03:2181")
  jobCfg.setOutputFormat(classOf[TableOutputFormat])
  jobCfg.set(TableOutputFormat.OUTPUT_TABLE,"tb_users")

  sc.parallelize(1 to 10 ,1).map(f=>{
    val put = new Put(s"spark${f+20}".getBytes)
    put.addColumn("info".getBytes,"name".getBytes,s"大褶子${f}".getBytes)
    put.addColumn("info".getBytes,"age".getBytes,s"${f+38}".getBytes)
    put.addColumn("info".getBytes,"sex".getBytes,"nv".getBytes)
    (new ImmutableBytesWritable(),put)
  }).saveAsHadoopDataset(jobCfg)
}
