# Spark summary：
# 本地搭建spark开发环境(主要包含以下六步)：


1.下载安装JDK1.8并配置环境变量(%JAVA_HOME%\bin 和 %JAVA_HOME%\jre\bin)；


2.下载安装scala-2.12(版本跟spark一致)并配置环境变量(%SCALA_HOME%\bin)；


3.下载安装hadoop(版本跟spark一致，这里选择hadoop-2.7.6.tar.gz)，解压到某个文件夹，然后配置环境变量(%HADOOP_HOME% 和 %HADOOP_HOME%\bin)，为了防止运行程序的时候出现nullpoint异常，下载hadoop.dll和winutils.exe, 然后复制到所安装hadoop的bin目录下；


4.官网下载spark对应版本的安装包(这里选择spark-2.3.0-bin-hadoop2.7.tgz)，解压到某个文件夹(如F:\spark)，然后配置环境变量(F:\spark\spark-2.3.0-bin-hadoop2.7\bin)，打开cmd，运行spark-shell正常启动spark，则spark安装完毕；


5.下载安装maven(apache-maven-3.5.4-bin.zip)，解压到某个文件夹，然后配置环境变量(%MAVEN_HOME%\bin)，cmd下用mvn -v查看Maven的版本信息


6.下载安装IDEA，首次启动之后需要先安装scala插件。安装完成后重启IDEA，否则无法配置全局scala SDK。配置好SDK(1.8)和global library(scala-sdk-2.11.12)之后就可以创建一个Maven项目，然后打包(mvn package打包)上传到集群上，提交任务(spark-submit)，你的代码就可以在集群上运行了，通过spark UI可以查看执行情况。





# # 流处理Streaming：


# 一、streaming需要具备的特点：


1.低延迟;
2.一致性语义保证(end-to-end exactly-once);
3.吞吐量;
4.乱序数据;
5.持久化的状态存储.


# 二、Streaming发展简史：


1.Storm是整个行业真正广泛采用的第一个流式处理系统：


1).满足低延迟;
2).弱一致性.


2.Spark streaming发布于2013年，它是流处理的分水岭，第一个公开可用的流处理引擎，可以提供批处理系统的正确性保证：
1).低延迟;
2).一致性语义保证(end-to-end exactly-once);
3).吞吐量;
4).有序数据（Structured Streaming 支持Event Time 与 Watermark）


3.Kafka不是数据计算框架，是一个持久的流式数据传输和存储工具，但它在所有系统中扮演了推动流处理的最有影响力的角色之一，为流处理领域做出了巨大贡献，可以说比其他任何单一系统都要多:


1).持久化的流式存储;
2).可重放.


大量流处理引擎依赖源头数据可重放来提供end-to-end exactly-once的计算保障


4.Flink在2015年突然出现在大数据舞台，它在不断提高整个行业的流计算处理标准：


1).低延迟
2).一致性语义保证 （end-to-end exactly-once）: Barrier
3).吞吐量
4).乱序数据(Watermark)
5).持久化的状态存储(snapshot、Savepoints)


# 三、streaming趋势：
批量和流式编程模型统一，简单易用，流处理逐渐代替批处理
