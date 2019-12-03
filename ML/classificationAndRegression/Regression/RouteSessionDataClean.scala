package com.yd.spark.clean
import com.yd.spark.util.DateUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.io.Source
object RouteSessionDataClean {

  /**
    * define schema of raw data
    */
  val schema = StructType(Array(
    StructField("id", StringType, true),
    StructField("limit_code", StringType, true),
    StructField("cargo_code", StringType, true),
    StructField("product_code", StringType, true),
    StructField("cons", StringType, true),
    StructField("currentaction", IntegerType, true),
    StructField("status", IntegerType, true),
    StructField("scantm", LongType, true),
    StructField("location", StringType, true),
    StructField("otherinfo", StringType, true),
    StructField("opattachinfo", StringType, true),
    StructField("scandt", StringType, true)
  ))

  case class Route(id:String,limit_code:String,	cargo_code:String,	product_code:String,cons:String,currentaction:Int,	status:Int,	scantm:Long,location:String,otherinfo:String,opattachinfo:String,scandt:String) extends Serializable

  case class Exp(
                      id:String,
                      limit_code:String,
                      cargo_code:String,
                      product_code:String,
                      currentcode:Int,
                      status:Int,
                      optm:Long,
                      location:String,
                      otherinfo:String,
                      opattachinfo:String,
                      opdt:String,
                      deliveryFlag:Int,retentionFlag:Int,backFlag:Int,delivery_tranship_flag:Int,pickup_tranship_flag:Int
                    ) extends Serializable

  case class SessionStay(
                      id:String,
                      limit_code:String,
                      cargo_code:String,
                      product_code:String,
                      location:String,
                      startOpcode:Int,
                      endOpcode:Int,
                      opflow:String,
                      startOptime:Long,
                      startOptm:String,
                      endOptime:Long,
                      endOptm:String,
                      remainMins:Double,
                      opattachinfo:String,
                      current_session_vehicle_flag:Int,
                      current_session_vehicle_exception_num:Int,
                      deliveryFlag:Int,retentionFlag:Int,backFlag:Int,delivery_tranship_flag:Int,pickup_tranship_flag:Int) extends Serializable

  case class SessionBetween(
                          id:String,
                          limit_type:String,
                          cargo_type:String,
                          product_code:String,
                          transType:String,
                          apattachinfo:String,
                          vehicleFlag:Int,
                          deliveryFlag:Int,
                          retentionFlag:Int,
                          backFlag:Int,
                          pickupTime:Long,
                          pickupTm:String,
                          dofW:Int,
                          dephour:Int,
                          deptime:Double,
                          dropoffTime:Long,
                          dropoffTm:String,
                          pickupCode:Int,
                          dropoffCode:Int,opflow:String,origin:String,dest:String,zoneflow:String,diff:Double) extends Serializable

  case class SessionBetweenResult(
                             id:String,
                             limit_type:String,
                             cargo_type:String,
                             product_code:String,
                             transType:String,
                             apattachinfo:String,
                             vehicleFlag:Int,
                             deliveryFlag:Int,
                             retentionFlag:Int,
                             backFlag:Int,
                             pickupTime:Long,
                             dofW:Int,
                             dephour:Int,
                             deptime:Double,
                             dropoffTime:Long,
                             pickupCode:Int,
                             dropoffCode:Int,opflow:String,origin:String,dest:String,zoneflow:String,diff:Double,
                             originT:String,destT:String,originLoc:Coord,destLoc:Coord,dist:Double) extends Serializable

  case class Coord(lng:String,lat:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RouteSessionDataClean")
    val sc = new SparkContext(conf)

    //broadcast area mapping csv to executor
    val conf_file = getClass.getResourceAsStream("/area_mapping20191115.csv")
    val area = Source.fromInputStream(conf_file).getLines().drop(1).map{row=>
      val fields = row.split(",")
      val id = fields(0)
      (id,row)
    }.toMap
    val broad_area = sc.broadcast(area)

    val spark = SparkSession.builder().appName("RouteSessionDataClean")
      .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    /**
      * load raw data from file
      */
    val file = "E:\\raw_waybill_data_20191010_20191013.csv"
    import spark.implicits._
    val df = spark.read
      .option("inferSchema","false")
      .option("header","true")
      .option("delimiter","|")
      .schema(schema)
      .csv(file)
      .as[Route]

    df.printSchema()
    df.show(5)
    df.createOrReplaceTempView("rawdata")


    /**
      * 清洗数据
      */
    val df1 = spark.sql("select * from rawdata where status != 2").rdd.map{line=>
      val id = line.getAs[String]("id")
      val limit_code = line.getAs[String]("limit_code").replace(",","")
      val cargo_code = line.getAs[String]("cargo_code").replace(",","")
      val product_code = line.getAs[String]("product_code").replace(",","")
      val currentaction = line.getAs[Int]("currentaction")
      val status = line.getAs[Int]("status")
      val optm = line.getAs[Long]("scantm")
      val location = line.getAs[String]("location").replace(",","")
      val otherinfo = if(line.get(9)!=null) line.getAs[String]("otherinfo").replace(",","") else ""
      val opattachinfo = if(line.get(10)!=null) line.getAs[String]("opattachinfo").replace(",","") else ""
      val opdt = line.getAs[String]("scandt").replace(",","")
      val value = id+"|"+limit_code+"|"+cargo_code+"|"+product_code+"|"+currentaction+"|"+status+"|"+optm+"|"+location+"|"+otherinfo+"|"+opattachinfo+"|"+opdt
      (id,value)
    }.reduceByKey((x,y)=>x + "," + y).map{line=>
      val list = line._2.split(",").sortWith(_.split("\\|")(6).toLong>_.split("\\|")(6).toLong)
      val deliveryFlag = if(list.exists(x=>x.split("\\|")(4) == "80"|| x.split("\\|")(4) == "125" || x.split("\\|")(4) == "130")) 1 else 0
      val retentionFlag = if(list.exists(x=>x.split("\\|")(4) == "70" || x.split("\\|")(4) == "33")) 1 else 0
      val backFlag = if(list.exists(x=>x.split("\\|")(4) == "99" || x.split("\\|")(4) == "95")) 1 else 0
      val delivery_tranship_flag = if(list.exists(x=>x.split("\\|")(4)=="47")) 1 else 0
      val pickup_tranship_flag = if(list.exists(x=>x.split("\\|")(4)=="407")) 1 else 0
      val end_list =list.filter(x=>x.split("\\|")(4) == "125" || x.split("\\|")(4) == "130" || x.split("\\|")(4) == "80")
      val last_row = if(end_list.length>0) end_list.last else list.head
      val first_list = list.filter(x=>x.split("\\|")(4) == "54" || x.split("\\|")(4) == "50" || x.split("\\|")(4) == "51")
      val first_row = if(first_list.length>0) first_list.last else list.last
      val merge_list = list.filter(x=>x.split("\\|")(4)!="47" && x.split("\\|")(4)!="407" && x.split("\\|")(4)!="95")
        .filter(x=>x.split("\\|")(6).toLong<=last_row.split("\\|")(6).toLong && x.split("\\|")(6).toLong>=first_row.split("\\|")(6).toLong)
          .map{x=>x+"|"+deliveryFlag+"|"+retentionFlag+"|"+backFlag+"|"+delivery_tranship_flag+"|"+pickup_tranship_flag}
      merge_list
    }.flatMap(x=>x)

    /**
      * 数据标准化为RDD[K,V],方便后续对数据做会话切分
      * K：id
      * V：为样例类Exp
      */
    val middle =df1.map{line=>
      val fields = line.split("\\|",-1)
      val id = fields(0)
      val limit_code = fields(1)
      val cargo_code = fields(2)
      val product_code = fields(3)
      val currentaction = fields(4).toInt
      val status = fields(5).toInt
      val optm = fields(6).toLong
      val location = fields(7)
      val otherinfo = fields(8)
      val opattachinfo = fields(9)
      val opdt = fields(10)
      val deliveryFlag = fields(11).toInt
      val retentionFlag = fields(12).toInt
      val backFlag = fields(13).toInt
      val delivery_tranship_flag = fields(14).toInt
      val pickup_tranship_flag = fields(15).toInt
      (waybill_no, Exp(id, limit_code, cargo_code, product_code, currentaction, status, optm, location, otherinfo, opattachinfo,
        opdt,deliveryFlag,retentionFlag,backFlag,delivery_tranship_flag,pickup_tranship_flag))
    }

    /**
      * 对数据做会话切分
      */
    val sessions = groupByKeyAndSortValues(middle,secondaryKeyFunc,split,100)
    sessions.cache()

    /**
      * 切分会话后，计算停留时长
      */
    val sessionsParsed = sessions.map(parseSession)
//    sessions.take(200).foreach(println)
//    sessionsParsed.take(200).foreach(println)

//    val sessionStay = sessionsParsed

    /**
      * 切分会话后，计算OD(origin-destination)间时长
      */
    val sessionBtween = sessionsParsed.map{session=>
      (session.waybill_no,session)
    }.groupByKey().map{x=>
//      val id = x._1
      val list = x._2.toList.sortWith(_.startOptime<_.startOptime)
//      val firstrow = list.head
//      val lastrow = list.last
//      val lifeCycle = (lastrow.endOptime-firstrow.startOptime)/(1000*60.0)
      val temp_list1 = list.dropRight(1)
      val temp_list2 = list.drop(1)
      val res = temp_list1.zip(temp_list2).map{case (a,b)=> merge(a,b)}
      res
    }.flatMap(x=>x)

//    sessionBtween.take(200).foreach(println)

    /**
      * data enrich
      * 增加OD间的球面距离
      */
    val sessionBtweenResult = sessionBtween.map{session=>
      val origin= session.origin
      val originT= broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(2)
      val lon1 = if(broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(3)=="N") broad_area.value.getOrElse(broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(1),"None").split(",")(3)  else broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(3)
      val lat1 = if(broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(4)=="N") broad_area.value.getOrElse(broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(1),"None").split(",")(4)  else broad_area.value.getOrElse(origin,"None,None,None,None,None").split(",")(4)
      val c1 = Coord(lon1,lat1)
      val dest = session.dest
      val destT= broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(2)
      val lon2 = if(broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(3)=="N") broad_area.value.getOrElse(broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(1),"None").split(",")(3)  else broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(3)
      val lat2 = if(broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(4)=="N") broad_area.value.getOrElse(broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(1),"None").split(",")(4)  else broad_area.value.getOrElse(dest,"None,None,None,None,None").split(",")(4)
      val c2 = Coord(lon2,lat2)
      val dist = haversine(c1,c2)
      SessionBetweenResult(session.waybill_no, session.limit_type, session.cargo_type, session.product_code, session.transType, session.apattachinfo, session.vehicleFlag,
        session.deliveryFlag, session.retentionFlag, session.backFlag, session.pickupTime, session.dofW, session.dephour, session.deptime, session.dropoffTime,
        session.pickupCode, session.dropoffCode,session.opflow,session.origin,session.dest,session.zoneflow,session.diff,originT,destT,c1,c2,dist)
    }.toDF()
    sessionBtweenResult.printSchema()
    sessionBtweenResult.show(20)
    sessionBtweenResult.createOrReplaceTempView("sessionBtween")

    val strain = sessionBtweenResult.select($"id",$"limit_type",$"cargo_type",$"product_code",$"pickupCode",
      $"origin",$"originT",$"dest",$"destT",$"transType",$"apattachinfo",$"dofW",$"dephour",$"deptime",$"diff",$"dist")
      .filter($"dist" < Double.PositiveInfinity)
      .filter($"vehicleFlag" < 1)
      .filter($"retentionFlag" < 1)
      .filter($"backFlag" < 1)

    /**
      * save clean data
      */
    strain.repartition(1).write.csv("C:\\xxx")


  }


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
        e.printStackTrace()
      }
    }
    dist
  }


  def merge(a:SessionStay,b:SessionStay):SessionBetween={
    val pickupTime = a.endOptime
    val pickupTm= a.endOptm
    /**
      * 时间特征
      */
    val dofW = DateUtil.getWeek(pickupTime)
    val dephour= pickupTm.split(" ")(1).split(":")(0).toInt
    val deptime = (pickupTm.split(" ")(1).split(":")(0).toInt.toString+pickupTm.split(" ")(1).split(":")(1).toInt.toString).toDouble
    val dropoffTime=b.startOptime
    val pickupCode= a.endOpcode
    val dropoffCode= b.startOpcode
    val opflow = pickupCode+"_"+dropoffCode
    val origin = a.zonecode
    val dest = b.zonecode
    val zoneflow = origin+"_"+dest
    val diff = (dropoffTime-pickupTime)/(1000*60.0)
    val id = a.id
    val limit_type= a.limit_code
    val cargo_type= a.cargo_code
    val product_code= a.product_code
    val transType = if(pickupCode == 570) "Train" else if(pickupCode == 105) "Flight" else "Route"
    val apattachinfo= a.opattachinfo
    val vehicleFlag = a.current_session_vehicle_flag
    val deliveryFlag= a.deliveryFlag
    val retentionFlag= a.retentionFlag
    val backFlag= a.backFlag
    SessionBetween(id, limit_type, cargo_type, product_code, transType, apattachinfo, vehicleFlag, deliveryFlag, retentionFlag,
      backFlag, pickupTime,pickupTm, dofW, dephour, deptime, dropoffTime, b.startOptm, pickupCode, dropoffCode,opflow,origin,dest,zoneflow,diff)
  }

  /**
    * 对运单路由数据做会话切分,返回SessionStay类，主要包含停留时长
    * @param line
    * @return
    */
  def parseSession(line:(String,List[Express])):SessionStay ={
    val id=line._1
    val sessionsList = line._2
    val current_session_vehicle_flag = if(sessionsList.exists(_.opcode==306)) 1 else 0
    val current_session_vehicle_exception_num = sessionsList.count(_.opcode==306)
    val tmp = sessionsList.filter(_.opcode!=306)
    val limit_code = sessionsList.head.limit_code
    val cargo_code = sessionsList.head.cargo_code
    val product_code = sessionsList.head.product_code
    val location = sessionsList.head.location
    val deliveryFlag = sessionsList.head.deliveryFlag
    val retentionFlag = sessionsList.head.retentionFlag
    val backFlag = sessionsList.head.backFlag
    val delivery_tranship_flag = sessionsList.head.delivery_tranship_flag
    val pickup_tranship_flag = sessionsList.head.pickup_tranship_flag
    val remain_minutes = if(tmp.length>1) (tmp.last.optm-tmp.head.optm)/(1000*60.0) else 0.0
    val remain_flag = if(tmp.length>1) tmp.head.currentaction+"_"+tmp.last.currentaction else sessionsList.head.currentaction+"_"+sessionsList.last.currentaction
    val firstRow = if(tmp.nonEmpty) tmp.head else sessionsList.head
    val lastRow = if(tmp.nonEmpty) tmp.last else sessionsList.last
    SessionStay(id, limit_code, cargo_code,product_code , location,
      firstRow.currentaction , lastRow.currentaction,remain_flag , firstRow.optm, firstRow.opdt, lastRow.optm,lastRow.opdt, remain_minutes, lastRow.opattachinfo,
        current_session_vehicle_flag,current_session_vehicle_exception_num,deliveryFlag, retentionFlag, backFlag,delivery_tranship_flag ,pickup_tranship_flag)
  }

  def secondaryKeyFunc(express:Express):Long = express.optm

  def split(e1:Express, e2:Express):Boolean ={
    val b1 = e1.optm
    val b2 = e2.optm
    val d = (b1-b2)/(1000*60*60)
    val loc1 = e1.location
    val loc2 = e2.location
    loc1!= loc2
  }

  /**
    * 二级排序
    * @param rdd
    * @param secondaryKeyFunc
    * @param splitFunc
    * @param numPartitions
    * @tparam K
    * @tparam V
    * @tparam S
    * @return
    */
  def groupByKeyAndSortValues[K : Ordering : ClassTag, V : ClassTag, S : Ordering](
                                                                                    rdd: RDD[(K, V)],
                                                                                    secondaryKeyFunc: (V) => S,
                                                                                    splitFunc: (V, V) => Boolean,
                                                                                    numPartitions: Int): RDD[(K, List[V])] = {
    val presess = rdd.map {
      case (lic, trip) => {
        ((lic, secondaryKeyFunc(trip)), trip)
      }
    }
    val partitioner = new FirstKeyPartitioner[K, S](numPartitions)
    presess.repartitionAndSortWithinPartitions(partitioner).mapPartitions(groupSorted(_, splitFunc))
  }

  def groupSorted[K, V, S](
                            it: Iterator[((K, S), V)],
                            splitFunc: (V, V) => Boolean): Iterator[(K, List[V])] = {
    val res = List[(K, ArrayBuffer[V])]()
    it.foldLeft(res)((list, next) => list match {
      case Nil =>
        val ((lic, _), trip) = next
        List((lic, ArrayBuffer(trip)))
      case cur :: rest =>
        val (curLic, items) = cur
        val ((lic, _), trip) = next
        if (!lic.equals(curLic) || splitFunc(items.last, trip)) {
          (lic, ArrayBuffer(trip)) :: list
        } else {
          items.append(trip)
          list
        }
    }).map { case (lic, buf) => (lic, buf.toList) }.iterator
  }

}

class FirstKeyPartitioner[K1, K2](partitions: Int) extends Partitioner {
  val delegate = new HashPartitioner(partitions)
  override def numPartitions:Int = delegate.numPartitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K1, K2)]
    delegate.getPartition(k._1)
  }
}
