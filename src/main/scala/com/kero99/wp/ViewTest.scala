package com.kero99.wp

/**
  *
  * @author wp
  * @date 2019-08-30 14:19
  *
  */
object ViewTest extends WpSession {
  import session.implicits._
  import session.sql
  val df = sc.parallelize(List("zhangsan"->11,"lisi"->22)).toDF("name","age").cache()
  df.createTempView("tb_u1")
  df.createGlobalTempView("tb_u2")

  sql("select * from tb_u1").show()
  sql("select * from global_temp.tb_u2").show()

  val s1 = session.newSession();
  s1.sql("select * from global_temp.tb_u2").show()
  s1.sql("select * from tb_u1").show()
}
