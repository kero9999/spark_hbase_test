package com.kero99.wp.hbase

import com.kero99.wp.WpSession
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

/**
  *
  * @author wp
  * @date 2019-08-30 15:04
  *
  */
object ReadTest extends WpSession{
  import session.implicits._
  import session.sql
  val cfg = HBaseUtils.getCfg()
  cfg.set(TableInputFormat.INPUT_TABLE,"tb_users")
  val rdd = sc.newAPIHadoopRDD(cfg,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).cache()

  val df = rdd.map(f=>{
//    println(new String(f._1.get()))
    val result = f._2
//    println(s"rowkey:${new String(result.getRow)}")
    val rowkey = Bytes.toString(result.getRow)
    println(s"rowkey:${rowkey}")

    //val cells = result.listCells()

    val map = new java.util.HashMap[String,String]()

    val scan=result.cellScanner()
    while(scan.advance()){
      val cell = scan.current()
      val cName = Bytes.toString(CellUtil.cloneQualifier(cell))
      val cValue = Bytes.toString(CellUtil.cloneValue(cell))
      map.put(cName,cValue)
    }
//    val name=Bytes.toString(CellUtil.cloneValue(cells.get(0)))
//    val age=Bytes.toString(CellUtil.cloneValue(cells.get(1)))
//    val sex=Bytes.toString(CellUtil.cloneValue(cells.get(2)))
//    for(i <- 0 until cells.size()){
//      val cell = cells.get(i)
//      println(new String(cell.getValueArray))
//      println(Bytes.toString(cell.getValueArray))
//      println(Bytes.toString(CellUtil.cloneValue(cell)))
//    }

    (rowkey,map.get("name"),map.get("age"),map.get("sex"))
  }).toDF("id","name","age","sex").cache()

  df.createOrReplaceTempView("tb_users")
  sql("select * from tb_users").show(1000)


}
