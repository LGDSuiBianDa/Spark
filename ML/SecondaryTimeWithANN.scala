package com.yd.spark


//import scala.collection.JavaConversions._

import com.yd.spark.util.{DateUtil, PropertiesLoader, ShardRedisProxyUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}




class SecondaryTimeWithANN {
//  val logger = org.apache.log4j.LogManager.getLogger("SecondaryTimeWithANN")


  /**
    * evaluate Secondary  Time with ANN 
    * @param id
    * @param dynamicTm
    */
  def evaluate(id:String,dynamicTm:Long): (Long,String) ={
    var plan_finish_time = dynamicTm
    var log_str = ""
    val requestTm = System.currentTimeMillis()
    if(id == null || id.length<10){
      throw new Exception(s"param id: $id or requestTm: $dynamicTm illegal,please Check!")
    }else{
      if(ShardRedisProxyUtils.exists(id)){
        try{
          val rCode = ShardRedisProxyUtils.hget(id, "rCode")
          val dzCode = ShardRedisProxyUtils.hget(id, "dzCode")
          val arriveTm = ShardRedisProxyUtils.hget(id, "czstTm").toLong
          val czCode = ShardRedisProxyUtils.hget(id, "czCode")
          val czCity = ShardRedisProxyUtils.hget(id, "czCity")
          val srCity = ShardRedisProxyUtils.hget(id, "srCity")
          val destCity = ShardRedisProxyUtils.hget(id, "dtCity")
          val p85 = ShardRedisProxyUtils.get("p_"+czCode).split("\\|")(0).toLong * 60 * 1000
          val p97 = ShardRedisProxyUtils.get("p_"+czCode).split("\\|")(1).toLong * 60 * 1000
          val stayTm = requestTm - arriveTm
          val scheduleTm = if(stayTm >= p85) arriveTm + p97 else arriveTm + p85
          val departureTm = if(scheduleTm <= requestTm) requestTm + 60*60*1000 else scheduleTm
          val dofW = DateUtil.getWeek(departureTm)
          val dephour = DateUtil.tranTimeToString(departureTm).split(" ")(1).split(":")(0).toInt
          val deptime = if (dephour == 0) DateUtil.tranTimeToString(departureTm).split(" ")(1).split(":")(1).toInt.toString.toDouble else (dephour.toString + DateUtil.tranTimeToString(departureTm).split(" ")(1).split(":")(1)).toDouble
          val suffix = (deptime/10).toInt*10
          val min = if(suffix<100) suffix else suffix-suffix/100*100
          val hour = suffix/100
          val head_rowkey = if(hour%2 == 0) min/10  else  min/10 + 6
          val prefix = if(head_rowkey<10) "0" + head_rowkey else head_rowkey.toString
          val status = 2 
//          val s = System.currentTimeMillis()
//          val pool = getRangePool(prefix ,status , rCode , czCode , dzCode, suffix)
          val pool = getRangePool1(prefix ,status , rCode , czCode , dzCode, suffix)
//          val e = System.currentTimeMillis()
//          logger.error(s"### 》 search use time: = ${(e - s) / 1000.0} 秒.")
//          logger.error(s"### 》 pool size = ${pool.length} ")
          val rowkey = prefix + status + rCode + czCode + dzCode + fixRowkeyPattern(suffix) // rowkey = 00 2 T77 579WA 010LC 2220
//          val pool = getHbaseRow(rowkey)
//          val confidence = getConfidence(czCode,dzCode,czCity,destCity,rCode,deptime.toInt,dofW)
          val confidence = getConfidence(czCode,dzCode,srCity,destCity,rCode,deptime.toInt,dofW)

          val predict = pakageSearchSecondaryTime(deptime, pool, rCode, czCity, destCity)
          if(pool.nonEmpty){
            log_str = if(log_str.length==0) log_str + s"success,$id,$dynamicTm,$rowkey,$requestTm,$departureTm,$predict,$confidence" else log_str+"|" + s"success,$id,$dynamicTm,$rowkey,$requestTm,$departureTm,$predict,$confidence"
          }else{
            log_str = if(log_str.length==0) log_str + s"hbase,$id,$dynamicTm,$rowkey,$requestTm,$departureTm,,$confidence" else log_str+"|" + s"hbase,$id,$dynamicTm,$rowkey,$requestTm,$departureTm,,$confidence"
          }
          //if 置信度>=0.8 延后2小时 else 延后6小时
          val res = if(confidence>=0.8) departureTm + predict * 60 * 1000L + 2*60*60*1000L else departureTm + predict * 60 * 1000L + 6*60*60*1000L
          plan_finish_time = if(Set(2315,1018).contains(predict))  dynamicTm else res

        }catch{case ex:Exception=>
          ex.printStackTrace()
//          logger.error(s"id: $id secondary  time failed! return default time: $dynamicTm")
        }
      }else{
        log_str = if(log_str.length==0) log_str + s"redis,$id,$dynamicTm,,,,," else log_str+"|" + s"redis,$id,$dynamicTm,,,,,"
//        logger.warn(s"id: $id can't find in redis! return default time: $dynamicTm")
      }
    }
    (plan_finish_time,log_str)
  }

  /**
    * get range pool
    * @param prefix
    * @param status
    * @param rCode
    * @param czCode
    * @param dzCode
    * @param slice
    * @return
    */
//  def getRangePool(prefix:String ,status:Int , rCode:String , czCode:String , dzCode:String, slice:Int): Array[(Int,Double)] ={
//    val bound1 = getlowerupperBound(slice,30)
//    val bound2 = getlowerupperBound(slice,60)
//    val bound3 = getlowerupperBound(slice,90)
//    val bound4 = getlowerupperBound(slice,120)
//
//    val rowkey1 = prefix + status + rCode + czCode + dzCode + bound1._1.toInt
//    val rowkey2 = prefix + status + rCode + czCode + dzCode + bound1._2.toInt
//    val rowkey3 = prefix + status + rCode + czCode + dzCode + bound2._1.toInt
//    val rowkey4 = prefix + status + rCode + czCode + dzCode + bound2._2.toInt
//    val rowkey5 = prefix + status + rCode + czCode + dzCode + bound3._1.toInt
//    val rowkey6 = prefix + status + rCode + czCode + dzCode + bound3._2.toInt
//    val rowkey7 = prefix + status + rCode + czCode + dzCode + bound4._1.toInt
//    val rowkey8 = prefix + status + rCode + czCode + dzCode + bound4._2.toInt
//    val pool1 = getHbaseRow(rowkey1)
//    val pool2 = getHbaseRow(rowkey2)
//    val pool3 = getHbaseRow(rowkey3)
//    val pool4 = getHbaseRow(rowkey4)
//    val pool5 = getHbaseRow(rowkey5)
//    val pool6 = getHbaseRow(rowkey6)
//    val pool7 = getHbaseRow(rowkey7)
//    val pool8 = getHbaseRow(rowkey8)
//    val rangePool = Array.concat(pool1,pool2,pool3,pool4,pool5,pool6,pool7,pool8)
//    rangePool
//  }

  def getRangePool1(prefix:String ,status:Int , rCode:String , czCode:String , dzCode:String, slice:Int): Array[(Int,Double)] ={
    val bound = getlowerupperBound(slice,120)
    val rowkey_pattern_begin = prefix + status + rCode + czCode + dzCode + fixRowkeyPattern(bound._1.toInt)
    val rowkey_pattern_end = prefix + status + rCode + czCode + dzCode + fixRowkeyPattern(bound._2.toInt)
    val rangePool = hbaseRowScan(rowkey_pattern_begin,rowkey_pattern_end)
    rangePool
  }

  def getlowerupperBound(slice:Int,step:Int):(Double,Double) ={
    val hour = if(slice/100<10) "0"+slice/100 else (slice/100).toString
    val minute = if(slice-(slice/100)*100<10) "0" + (slice-(slice/100)*100) else (slice-(slice/100)*100).toString
    val current_time = "2020-09-01 " + hour +":"+ minute + ":00"
    val time_begin = DateUtil.tranTimeToString(DateUtil.tranTimeToLong(current_time) - step*60*1000)
    val time_begin_hour= time_begin.split(" ")(1).split(":")(0).toInt
    val lowerBound = if(time_begin_hour == 0) time_begin.split(" ")(1).split(":")(1).toInt.toString.toDouble else (time_begin_hour.toString+time_begin.split(" ")(1).split(":")(1)).toDouble
    val time_end = DateUtil.tranTimeToString(DateUtil.tranTimeToLong(current_time) + step*60*1000)
    val time_end_hour = time_end.split(" ")(1).split(":")(0).toInt
    val upperBound = if(time_end_hour == 0) time_end.split(" ")(1).split(":")(1).toInt.toString.toDouble else (time_end_hour.toString+time_end.split(" ")(1).split(":")(1)).toDouble
    (lowerBound,upperBound)
  }


  /**
    *
    * @param rowkey_pattern_begin
    * @param rowkey_pattern_end
    * @return
    */
  def hbaseRowScan(rowkey_pattern_begin:String,rowkey_pattern_end:String):Array[(Int,Double)] ={

    val quorum = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.quorum")
    val port = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.port")
    val hbconf= HBaseConfiguration.create()
    hbconf.setInt("hbase.rpc.timeout",20000)
    hbconf.setInt("hbase.client.operation.timeout",20000)
    hbconf.setInt("hbase.client.scanner.timeout.period",20000)
    hbconf.set(TableInputFormat.INPUT_TABLE,"xxx")
    hbconf.set("hbase.zookeeper.quorum",quorum)
    hbconf.set("hbase.zookeeper.property.clientPort",port)
    hbconf.set("zookeeper.znode.parent", "/hbase")
    val table = new HTable(hbconf,"xxx")
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(rowkey_pattern_begin))
    scan.setStopRow(Bytes.toBytes(rowkey_pattern_end+"Z"))
    val rs = table.getScanner(scan)
    val it = rs.iterator()
    val list = new java.util.ArrayList[String]()
    while (it.hasNext){
      val n = it.next()
      val rsMap = n.getNoVersionMap
      val infoMap = rsMap.get(Bytes.toBytes("f"))
      val value = Bytes.toString(infoMap.get(Bytes.toBytes("q")))
      list.add(trimComma(value))
    }
    rs.close()
//    logger.error(s"### 》 $rowkey_pattern_begin - $rowkey_pattern_end pool size = ${list.toArray().size} ,pool = ${list.toArray().toList.toString()}")

    val pool:Array[(Int,Double)] = if(list.toArray().length<1) Array() else list.toArray().mkString(",").split(",").map{row=>
      val fields = row.split("\\|",-1)
      (fields(0).toInt,fields(1).toDouble)
    }
    pool
  }

  /**
    * get pool
    * @param rowkey
    * @return
    */
  def getHbaseRow(rowkey:String):Array[(Int,Double)] ={
    val quorum = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.quorum")
    val port = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.port")
    val hbconf= HBaseConfiguration.create()
    hbconf.setInt("hbase.rpc.timeout",20000)
    hbconf.setInt("hbase.client.operation.timeout",20000)
    hbconf.setInt("hbase.client.scanner.timeout.period",20000)
    hbconf.set("hbase.zookeeper.quorum",quorum)
    hbconf.set("hbase.zookeeper.property.clientPort",port)
    hbconf.set("zookeeper.znode.parent", "/hbase")
    val connection = ConnectionFactory.createConnection(hbconf)
    val table = connection.getTable(TableName.valueOf("xxx"))
    var pool:Array[(Int,Double)] = Array()
    try{
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"))
      val result = table.get(get)
      val value = Bytes.toString(result.value())
      if(value != null && value != "null" ){
        pool = trimComma(value).split(",").map{row=>
          val fields = row.split("\\|",-1)
          (fields(0).toInt,fields(1).toDouble)
        }
      }
    }catch{case e:Exception=>e.printStackTrace()
//      logger.error(s"hbase get error! ${e.getMessage}")
    }
    table.close()
    connection.close()
    pool
  }

  /**
    * trim Comma
    * @param in
    * @return
    */
  def trimComma(in:String): String ={
    val begin = in.indexOf(",")
    val end = in.lastIndexOf(",")
    var res = in
    if(begin != end){
      res = in.substring(begin + 1, end)
    }else{
      if(begin == 0 && begin == end){
        res = in.replace(",","")
      }
    }
    res
  }

  /**
    * get distance
    * @param x
    * @param y
    * @return
    */
  def getDistance(x: Array[Double], y: Array[Double]): Double =
    math.sqrt(x.zip(y).map(elem => math.pow(elem._1 - elem._2, 2)).sum)

  /**
    * ANN
    * @param target
    * @param pool
    * @param r_c
    * @param current_city
    * @param d_c
    * @return
    */
  def pakageSearchSecondaryTime(target: Double,
                                    pool: Array[(Int,Double)],
                                    r_c:String,
                                    current_city:String,d_c:String):Int = {
    val predict = if(pool.nonEmpty) {
      val pool2 = pool.map(t => (getDistance(Array(target), Array(t._2)), t)).sortWith(_._1<_._1)
      val pool3 = pool2.head
      val diff = pool3._2._1
      diff
    }else{
      val default = if(r_c == "x" || r_c == "y") 2315 else 1018
      default
    }
    predict
  }

  /**
    * get confidence
    * @param c_z_c
    * @param d_z_c
    * @param s_c
    * @param d_c
    * @param r_c
    * @param deptime
    * @param dofw
    * @return
    */
  def getConfidence(c_z_c:String,
                    d_z_c:String,
                    s_c:String,
                    d_c:String,
                    r_c:String,
                    deptime:Int,
                    dofw:Int): Double ={

    //load model weights and intercept
//    logger.info(s"load model parameters:")
//    val p_model=LogisticRegressionModel.load(sc,"hdfs://xxx//xxx")

    val intercept=ShardRedisProxyUtils.get("model_intercept").toDouble
    val weight = ShardRedisProxyUtils.hgetAll("model_weights")
//    val weight_list=weight.toArray.map(t=>(t._1.toInt,t._2.toDouble)).toMap
    val es = weight.keySet().iterator()
    val weight_list = new java.util.HashMap[Int,Double]()

//    var weight_list:scala.collection.mutable.Map[Int,Double] =mutable.Map()
    while (es.hasNext){
      val k = es.next()
      weight_list.put(k.toInt,weight.get(k).toDouble)
//      weight_list += (k.toInt -> weight.get(k).toDouble)
    }


    val dict_curr_size=ShardRedisProxyUtils.get("dict_curr_size").toInt
    val dict_dzc_size=ShardRedisProxyUtils.get("dict_dzc_size").toInt
    val dict_sc_size=ShardRedisProxyUtils.get("dict_sc_size").toInt
    val dict_dc_size=ShardRedisProxyUtils.get("dict_dc_size").toInt
    val dict_rc_size=ShardRedisProxyUtils.get("dict_rc_size").toInt

//    val feature = connectSparseVector(List(
//      factorToVector("dict_curr",dict_curr_size ,c_z_c),
//      factorToVector("dict_dzc",dict_dzc_size, d_z_c),
//      factorToVector("dict_sc",dict_sc_size, s_c),
//      factorToVector("dict_dc",dict_dc_size,d_c ),
//      factorToVector("dict_rc",dict_rc_size,r_c),
//      new DenseVector(Array(deptime)).toSparse,
//      new DenseVector(Array(dofw)).toSparse))


    val feature = connectList(List(
      factorToList("dict_curr",dict_curr_size ,c_z_c),
      factorToList("dict_dzc",dict_dzc_size, d_z_c),
      factorToList("dict_sc",dict_sc_size, s_c),
      factorToList("dict_dc",dict_dc_size,d_c ),
      factorToList("dict_rc",dict_rc_size,r_c),
      List(deptime.toDouble),
      List(dofw.toDouble)
    ))

//    logger.error(s"### 》 feature size = ${feature.size} ,pool = ${feature} ,.indices = ${feature.indices.toList}")
//    logger.error(s"### 》 feature1 size = ${feature1.size} ,pool = ${feature1} .indices = ${feature1.map(t=>if(t>0) feature1.indexOf(t).toInt else 0)}")

//    val confidence = p_model.predict(feature)
//    val confidence = 1/(1+math.exp(-(List(intercept):::weight_list).zip(List(1.0):::feature.toArray.toList).map(p => (p._1*p._2)).sum))
//    val confidence = 1/(1+math.exp(-(List(intercept):::feature.indices.map(l=>weight_list.getOrDefault(l,0.0)).toList).zip(List(1.0):::feature.values.toList).map(p => p._1*p._2).sum))
    val confidence = 1/(1+math.exp(-(List(intercept):::feature.map(t=> if(t!=0.0) weight_list.getOrDefault(feature.indexOf(t),0.0) else 0.0)).zip(List(1.0):::feature).map(p => p._1*p._2).sum))

//    logger.error(s"### 》 confidence size = ${confidence} ,pool = ${confidence}")
//    logger.error(s"### 》 confidence1 size = ${confidence1} ,pool = ${confidence1}")


    confidence
  }

//  def factorToVector(in:String,dict_size:Int,dict_name:String)={
//    if(ShardRedisProxyUtils.hexists(in,dict_name)){new SparseVector(dict_size, Array(ShardRedisProxyUtils.hget(in,dict_name).toInt), Array(1.0))}
//    else{new SparseVector(dict_size, Array(1).drop(1), Array(1.0).drop(1))}
//  }
//
//  def connectSparseVector(in:List[SparseVector])={
//    val sizes=List(0):::in.map(_.size).dropRight(1)
//    val indices=(1 to in.size).toList.map(t => sizes.take(t).sum).zip(in.map(_.indices)).flatMap(t => t._2.map(_+t._1)).toArray
//    new SparseVector(in.map(_.size).sum,indices,in.toArray.flatMap(_.values))
//  }

  def connectList(in:List[List[Double]])={
    in.foldLeft(List(0.0).drop(1))(_:::_)
  }

  def factorToList(in:String,dict_size:Int,dict_name:String)={
    val out=new Array[Double](dict_size)
//    logger.error(s"### 》 dict_size size = ${dict_size} ,index = ${ShardRedisProxyUtils.hget(in,dict_name)}")
    if(ShardRedisProxyUtils.hexists(in,dict_name)){out(ShardRedisProxyUtils.hget(in,dict_name).toInt)=1.0}
//    val out=new Array[Double](dict.size)
//    if(dict.contains(in)){out(dict(in))=1.0}
    out.toList
  }

  def fixRowkeyPattern(suffix:Int): String ={
    val res = if(suffix.toString.length<4) new Array[Int](4-suffix.toString.length).mkString("")+suffix else suffix.toString
    res
  }

}


object SecondaryTimeWithANN{
  def main(args: Array[String]): Unit = {
//    val id = args(0)
//    val dynamic_tm = args(1)
//    val sdt = new SecondaryTimeWithANN
//    val res = sdt.evaluate(id,DateUtil.tranTimeToLong(dynamic_tm))
    val id = "1111111112"
    val dynamic_tm = 1608202295546L
    val s = System.currentTimeMillis()
    val res = (new SecondaryTimeWithANN).evaluate(id,dynamic_tm)
    val e = System.currentTimeMillis()
    println(s"### 》search use time: = ${(e - s) / 1000.0} 秒.")
    println(s"id : $id ,result = $res")
  }
//  for(i<- 1 to 30){
//    val r = scala.util.Random.nextInt(2)
//    println(s"random = $r")
//  }


}

