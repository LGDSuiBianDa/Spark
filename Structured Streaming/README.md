# # 流处理：



# 一、Streaming发展简史：
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


# 二、streaming需要具备的特点：
1.低延迟;


2.一致性语义保证(end-to-end exactly-once);


3.吞吐量;


4.乱序数据;


5.持久化的状态存储.


# 三、streaming趋势：
批量和流式编程模型统一，简单易用，流处理逐渐代替批处理


## 总结：

# streaming流处理的一般过程包括以下几步：
1.实时数据源接入（一般来源是kafka，本地可以用socket nc  测试小工具发送数据到spark streaming,来验证计算逻辑准确性）


2.探索性数据分析（一般来说如果你清楚地知道当前已有数据的结构，那么后续的分析或计算是水到渠成的；但是如果不知道当前已有数据的结构，就需要进行探索性数据分析）
no matter train a model or build a streaming task , EDA (Exploratory Data Analysis) is one of the basic and necessary step among the streaming or ML pipeline.


3.流处理编程模型
图片 2.png

这一步包含如下事项：

(1).一系列转换操作;

(2).状态管理;

(3).输出操作（output sink、output mode）---输出介质和输出模式的确定;

上述3点一般适用于普通实时任务，当然实时任务中还可以加入模型来做预测。


4.准确性验证（如果第三步包含了模型，那么除了验证准确性之外，还需要分析验证模型的预测结果）


5.部署

图片 1.png


# 开发一个streaming任务不是什么特别困难的事情，如何保证你的streaming任务结果准确，并且可以7*24不间断提供服务，这无疑是比较有挑战的：
1.开发阶段的结果准确性可以通过如下方法验证：

1).使用windows环境下的socket nc  测试小工具nc -L -p 9999  在windows本地 进行测试，发送socket包到spark streaming,验证逻辑准确性.
执行 nc -L -p 9999 -v ,手工输入数据,或者导入一个文件(nc -L -p 9999 -v < G:\test_data\part-00000).

2).除了socket方式还可以本地安装Kafka并通过spark实时计算来消费Kafka数据从而验证计算逻辑（mac版本），一般为以下几个步骤：

(1).启动zookeeper、Kafka

brew services start zookeeper
brew services start kafka
首次需要先创建topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic ITMS_CHILD_PARENT_SCHEDULE
查看创建的topic
kafka-topics --list --zookeeper localhost:2181

(2).KafkaMsgProducer发送测试数据到topic

也可以在终端上用下面的命令发送单条数据到指定Kafka
kafka-console-producer --broker-list localhost:9092 --topic ITMS_CHILD_PARENT_SCHEDULE

(3).实时任务消费Kafka数据，验证计算逻辑

(4).关闭zookeeper、Kafka

brew services stop zookeeper
brew services stop kafka

3).基于相同逻辑的离线版本跟实时版本结果对比


2.7*24不间断提供服务即运行过程中的稳定性、准确性需要通过checkpoint来作保证（异常恢复）
