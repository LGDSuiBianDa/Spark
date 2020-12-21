package com.yd.spark.clean

import java.util.Date

import com.yd.spark.util.{ChineseCalendarUtils, DateUtil, PropertiesLoader, SparkUtil}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Get, Put, Scan, Table}
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.util.Random

object CleanData2Hbase {
  val logger = LogManager.getLogger("CleanData2Hbase")

  def main(args: Array[String]): Unit = {

    /**
     * args1: begin date
     * args2: end date
     */

    var current_date = DateUtil.getYesterday().replace("-","")
    var start_date = DateUtil.getDatesBefore(current_date,3).split(" ")(0).replace("-","")
    if(args.length >= 1){
      current_date = args(0)
      start_date = args(1)
    } else {
      System.out.println(s"## Using default date parameters: $start_date,between $current_date .  unless you specify the specific date. For example: 20200101")
    }


    /**
     *  set user.name for hive sit environment
     */
    //    System.setProperty("user.name", "hdfs")


    /**
     * spark session
     */
    val conf = SparkUtil.initSparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Map[String, Int]],classOf[java.util.Date],classOf[Array[Double]],
      classOf[Broadcast[Map[String,String]]],classOf[Broadcast[Map[String,Int]]], classOf[Array[Byte]],classOf[String]))
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    System.out.println(s"## Use date   $current_date  " +
      s" to generate the current batch data as a part of data input. ")

    /**
     * 读取拉链表
     */
    val area_mapping = spark.sql("select dp_code,parent_dp_code,dp_type_code,longitude,latitude,currency_code from table_name_a")
    area_mapping.printSchema()
    area_mapping.show(5)
    val area_mapping_rdd = area_mapping.rdd.map{line=>
      val dp_code = line.getAs[String](0)
      val value = line.mkString(",")
      (dp_code,value)
    }.collect().toMap
    val broad_area = sc.broadcast(area_mapping_rdd)

    /**
      * 将两周历史数据清洗后写入hbase
      */
    val all_nodes_middle_data = spark.sql("select * from table_name_b " +
      " where inc_day = '"+current_date+"'"
    ).rdd.map{r=>
      var uid = "NA"
      var s_z_c = "NA"
      var c_z_c = "NA"
      var d_z_c = "NA"
      var r_c = "NA"
      var stay_time =0.0
      var total_diff =0.0
      var dofW = 9
      var dephour = 24
      var work_day = 2
      var deptime = 0.0
      var r_c_c = "NA"
      var qty = 0.0
      var r_a_f = 0.0
      var i_m_w = 2
      var dist = 0.0
      var stage_end_tmlong = 0L
      var real_finish_time = 0L
      try{
        uid = r.getAs[String]("uid")
        s_z_c = r.getAs[String]("s_z_c")
        c_z_c = r.getAs[String]("z_c")
        d_z_c = r.getAs[String]("d_z_c")
        r_c = r.getAs[String]("r_c")
        real_finish_time = r.getAs[Long]("real_finish_time")
        stage_end_tmlong = r.getAs[Long]("stage_end_tmlong")
        total_diff =  (real_finish_time-stage_end_tmlong)/(1000*60*1.0)
        stay_time =  r.getAs[Double]("stay_time")
        dofW = DateUtil.getWeek(stage_end_tmlong)
        dephour = DateUtil.tranTimeToString(stage_end_tmlong).split(" ")(1).split(":")(0).toInt
        deptime = if (dephour == 0) DateUtil.tranTimeToString(stage_end_tmlong).split(" ")(1).split(":")(1).toInt.toString.toDouble else (dephour.toString + DateUtil.tranTimeToString(stage_end_tmlong).split(" ")(1).split(":")(1)).toDouble
        work_day=ChineseCalendarUtils.isHoliday(DateUtil.tranTimeToString(stage_end_tmlong).split(' ')(0))
        r_c_c = r.getAs[String]("r_c_c")
        qty = r.getAs[String]("qty").toDouble
        r_a_f = r.getAs[Double]("f")
        i_m_w = if(r.getAs[Double]("quantity")>1) 1 else 0
        val lon1 = broad_area.value.getOrElse(r.getAs[String]("s_z_c"),"None,None,None,None,None,None").split(",")(3)
        val lat1 = broad_area.value.getOrElse(r.getAs[String]("s_z_c"),"None,None,None,None,None,None").split(",")(4)
        val lon2 = broad_area.value.getOrElse(r.getAs[String]("d_z_c"),"None,None,None,None,None,None").split(",")(3)
        val lat2 = broad_area.value.getOrElse(r.getAs[String]("d_z_c"),"None,None,None,None,None,None").split(",")(4)
        val c1 = Coord(lon1,lat1)
        val c2 = Coord(lon2,lat2)
        dist = haversine(c1,c2)
      }catch{case ex:Exception=>
        ex.printStackTrace()
      }
      BaseRecords(uid,s_z_c,c_z_c,d_z_c,r_c,i_m_w,stage_end_tmlong,dofW,dephour,deptime,work_day,qty,r_a_f,r_c_c,dist,stay_time,total_diff,real_finish_time)
    }.filter{t=>
      t.uid.length>0 && t.dofW<9 && t.dephour<24 && t.work_day<2 && t.stay_time>0 && t.total_diff>0
    }.distinct().persist(StorageLevel.DISK_ONLY)


    val flow_percentile = all_nodes_middle_data.map(x=>((x.c_z_c,x.d_z_c,x.r_c),(x,x.total_diff))).groupByKey().map{t=>
      val pool = t._2.toArray
      val p_msg = getMedian(pool.map(_._2))
      val p25 = p_msg._1
      val median = p_msg._2
      val p75 = p_msg._3
      val threshold = if((p75-p25)*2 + p75 < 7200) (p75-p25)*2 + p75 else 7200
      val pool1 = pool.filter(x=>x._2<=threshold && x._2>=180)
      pool1.map(_._1)
    }.flatMap(x=>x).persist(StorageLevel.DISK_ONLY)

    val train_middle_tmp1 = flow_percentile.map{tr=>
      val d_z_c = tr.d_z_c
      val c_z_c = tr.c_z_c
      val work_day = tr.work_day
      val total_diff = tr.total_diff.round
      val r_c = tr.r_c
      val deptime = tr.deptime
      //      ((first_trans_code,d_z_c,r_c,work_day),(uid,qty,r_c_c,stay_time,deptime))
      ((c_z_c,d_z_c,r_c,work_day),(total_diff,deptime))
    }.groupByKey()

    /**
      * generatePoolTailrec
      */
    val result1 = train_middle_tmp1.map{case(k,v)=>
      val iter = v.toList.distinct
      val fields = iter.map(x=>(x._2/10).toInt*10).distinct
      fields.map(t=>(k,t,generatePoolTailrec(8,iter,t)))
    }.flatMap(x=>x).filter(_._3.nonEmpty)
    result1.persist(StorageLevel.DISK_ONLY)

    /**
      * 每个region rowkey散列分布
      */
    result1.map{record=>
      val min = if(record._2<100) record._2 else record._2-record._2/100*100
      val hour = record._2/100
      val head_rowkey = if(hour%2 == 0) min/10  else  min/10 + 6
      val prefix = if(head_rowkey<10) "0"+head_rowkey else head_rowkey.toString
      (prefix,1)
    }.reduceByKey(_+_).take(20).foreach(println)


    /**
      * 批量写、删除
      */
    val s = System.currentTimeMillis()
    result1.repartition(1000).foreachPartition{partition=>
      val quorum = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.quorum")
      val port = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.port")
      val hbconf= HBaseConfiguration.create()
      hbconf.set("hbase.zookeeper.quorum",quorum)
      hbconf.set("hbase.zookeeper.property.clientPort",port)
      hbconf.set("zookeeper.znode.parent", "/hbase")
      val connection = ConnectionFactory.createConnection(hbconf)
      val table = connection.getTable(TableName.valueOf("XXX"))

      partition.foreach{record=>

        val min = if(record._2<100) record._2 else record._2-record._2/100*100
        val hour = record._2/100
        val head_rowkey = if(hour%2 == 0) min/10  else  min/10 + 6
        val prefix = if(head_rowkey<10) "0" + head_rowkey else head_rowkey.toString

        val rowkey = prefix + "4" + record._1._3 + record._1._1 + record._1._2 + fixRowkeyPattern(record._2)
        var pool:Array[String] = Array()
        try{
          val get = new Get(Bytes.toBytes(rowkey))
          get.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"))
          val result = table.get(get)
          val value = Bytes.toString(result.value())
          if(value != null && value != "null" ){
            pool = value.split(",")
          }
        }catch{case e:Exception=>
          logger.error(s"hbase get error! ${e.getMessage}")
        }

        val put= new Put(Bytes.toBytes(rowkey))
        val left_size = 2000-record._3.length
        val origin_size = pool.length
        val value = if(origin_size == 0)
          record._3.map(t=>t._1+"|"+t._2).mkString(",")
        else if(origin_size <= left_size)
          record._3.map(t=>t._1+"|"+t._2).mkString(",") +","+ pool.mkString(",")
        else
          record._3.map(t=>t._1+"|"+t._2).mkString(",") +","+ pool.drop(origin_size-left_size).mkString(",")
        put.add(Bytes.toBytes("f"),Bytes.toBytes("q"),Bytes.toBytes(value))
        table.put(put)
        println(" ===>> hbase put finished")

      }
      table.close()
      connection.close()
      Thread.sleep(3000)
    }
    val e = System.currentTimeMillis()
    println("总共用时:"+ (e-s)/1000.0 +"秒")



    sc.stop()

  }

  def fixRowkeyPattern(suffix:Int): String ={
    val res = if(suffix.toString.length<4) new Array[Int](4-suffix.toString.length).mkString("")+suffix else suffix.toString
    res
  }

  /**
   * 删除hdfs目录
   * @param sc
   * @param outputPath
   */
  def deleteOutPutPath(sc: SparkContext,outputPath: String):Unit={
    val path = new Path(outputPath)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path,true)
    }
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
   * 迭代器:每10分钟迭代一次 累加器：某个时间段的任务列表
   * @param n
   * @param ll
   * @param slice
   * @return
   */
  def generatePoolTailrec(n:Int,ll:List[(Long,Double)],slice:Int):List[(Long,Double)] ={
    @tailrec
    val r = new Random(1234L)
    val lowerBound = getlowerupperBound(slice,10)._1
    val upperBound = getlowerupperBound(slice,10)._2
    var iter = if(lowerBound<upperBound) ll.filter(x=>x._2 >= lowerBound && x._2 <= upperBound) else ll.filter(x=>x._2 >= lowerBound || x._2 <= upperBound)
    if(iter.length>100){
      iter = r.shuffle(iter).take(100)
    }

    def generatePoolTailrec(min:Int,result:List[(Long,Double)],pool:List[(Long,Double)],diff:Int): List[(Long,Double)] ={
      if(min == 0 || result.length == 100){
        result
      } else{
        val new_lowerBound = getlowerupperBound(slice,10+diff)._1
        val new_upperBound = getlowerupperBound(slice,10+diff)._2
        val previous_lowerBound = getlowerupperBound(slice,diff)._1
        val previous_upperBound = getlowerupperBound(slice,diff)._2
        val tmp1 = if(new_lowerBound<previous_lowerBound) pool.filter(x=>x._2>new_lowerBound && x._2<previous_lowerBound) else pool.filter(x=>x._2>new_lowerBound || x._2<previous_lowerBound)
        val tmp2 = if(previous_upperBound<new_upperBound) pool.filter(x=>x._2>previous_upperBound && x._2<new_upperBound ) else pool.filter(x=>x._2>previous_upperBound || x._2<new_upperBound )
        val tmp = tmp1 ++ tmp2
        val temp = r.shuffle(tmp).take(100-result.length)
        generatePoolTailrec(min-1,result ++ temp,pool,diff+10)
      }
    }
    /**
     * 如果abs(30)样本不够2W，那么按每10分钟一个间隔前后扩至1小时，使得样本量接近2W
     */
    generatePoolTailrec(n,iter,ll,10)
  }



  def getMedian(arr:Array[Double]) ={
    val sorted = arr.sortBy(identity).zipWithIndex.map {
      case (v, idx) => (idx, v)
    }

    val count = sorted.length

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.filter(_._1==l).head._2 + sorted.filter(_._1 == r).head._2) / 2
    } else sorted.filter(_._1 == (count / 2)).head._2
    val p25 = sorted.filter(_._1 == (count * 0.25).toInt).head._2
    val p75 = sorted.filter(_._1 == (count * 0.75).toInt).head._2
    (p25,median,p75)
  }




  case class BaseRecords(
                           uid:String,s_z_c:String,c_z_c:String,d_z_c:String,r_c:String,
                           i_m_w:Int,
                           stage_end_tmlong:Long,dofW:Int,dephour:Int,deptime:Double,work_day:Int,
                           qty:Double,
                           r_a_f:Double,
                           r_c_c:String,
                           dist:Double,
                           stay_time:Double,total_diff:Double,real_finish_time:Long)



  case class Coord(lng:String,lat:String)

  def toRadians(degree:Double):Double ={
    degree * Math.PI / 180
  }

  /**
   * 两个经纬度间的球面距离
   * @param coord1 坐标1
   * @param coord2 坐标2
   * @return 球面距离（单位：米）
   */
  def haversine(coord1: Coord,coord2: Coord): Double ={
    var dist=Double.PositiveInfinity
    val R = 6371
    if(coord1.lng!=None.toString&&coord1.lat!=None.toString&&coord2.lng!=None.toString&&coord2.lat!=None.toString&&coord1.lng!="N"&&coord1.lat!="N"&&coord2.lng!="N"&&coord2.lat!="N"){
      try{
        val dlat = toRadians(coord2.lat.toDouble-coord1.lat.toDouble)
        val dlng = toRadians(coord2.lng.toDouble-coord1.lng.toDouble)
        val latitude1=toRadians(coord1.lat.toDouble)
        val latitude2 =toRadians(coord2.lat.toDouble)
        val a=Math.pow(Math.sin(dlat/2),2) + Math.cos(latitude1) * Math.cos(latitude2) * Math.pow(Math.sin(dlng/2),2)
        val c=2 * Math.asin(Math.sqrt(a))
        dist=R*c*1000
      }catch{case e:Exception=>
        //        log.error("coord1:  "+coord1+" ,coord2: "+coord2 +"  is illegal! Please check input coord!  "+ "Error message is: "+e.getMessage)
        //        e.printStackTrace()
      }
    }
    dist
  }


}
