package com.kero99.wp.hbase

import com.kero99.wp.WpSession
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.mapreduce.Job
/**
  *
  * @author wp
  * @date 2019-08-30 16:17
  *
  */
object WriteTest03 extends WpSession{
  val htable = HBaseUtils.getTable("tb_users")
  val cfg = HBaseUtils.getCfg()
  val job = Job.getInstance(cfg)
  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setMapOutputValueClass(classOf[KeyValue])

  HFileOutputFormat2.configureIncrementalLoadMap(job,htable)
  val rdd = sc.parallelize(1 to 10,1).map(f=>{
    val kv = new KeyValue(s"spark%03d".format(f).getBytes,"info".getBytes,"name".getBytes,s"张寒${f}".getBytes)
    (new ImmutableBytesWritable(),kv)
  })
  //RDD 转换成 HFile 并存储到HDFS上
  rdd.saveAsNewAPIHadoopFile("hdfs://apache01:9000/tmp/hbase",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],cfg)
  //将HFile 添加到 HBase中
  //获取tb_users 表所在的region
  val regionLocator=HBaseUtils.getConn().getRegionLocator(htable.getName)
  val bulkLoader = new LoadIncrementalHFiles(cfg)
  bulkLoader.doBulkLoad(new Path("hdfs://apache01:9000/tmp/hbase"),HBaseUtils.getAdmin(),htable,regionLocator)


  /**
    * java.io.IOException: Added a key not lexically larger than previous. Current cell = spark10/info:name/1567154297750/Put/vlen=8/seqid=0, lastCell = spark9/info:name/1567154297750/Put/vlen=7/seqid=0
    */
}
