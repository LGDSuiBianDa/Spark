# 加载配置文件注意事项：
配置文件一般放在src/main/resources文件夹下，针对不同类型的配置文件spark有不同方式来读取：

1.key-value类型的配置，一般为.properties文件

PropertiesLoader.getInstance().getProperty("paramName"）

2.普通文本，可以为csv/txt,文件大小不超过500M，通过如下方式读取，并广播出去。

val file = getClass.getResourceAsStream("/config-area.properties")

val areaMap = Source.fromInputStream(file).getLines().toArray.map{row=>
  val fields = row.split(",",-1)
  val dept_code =fields(0)
  val zzc_code = fields(1)
  (dept_code,zzc_code)
}.toMap

val area_mapping = sc.broadcast(areaMap)

area_mapping.value.getOrElse()

需要注意的是，上述方式maven pom文件<build></build>里要加上下面内容

 <resources>
      <resource>
        <directory>src/main/resources</directory>
        <includes>
          <include>*.csv</include>
          <include>*.xml</include>
          <include>*.properties</include>
        </includes>
        <filtering>true</filtering>
      </resource>
 </resources>
