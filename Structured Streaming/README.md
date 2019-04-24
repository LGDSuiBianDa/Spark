本地安装Kafka并用spark实时计算来消费，一般为以下几个步骤：

1.启动zookeeper、Kafka

brew services start zookeeper

brew services start kafka

首次需要先创建topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic ITMS_CHILD_PARENT_SCHEDULE

查看创建的topic
kafka-topics --list --zookeeper localhost:2181

2.KafkaMsgProducer发送测试数据到topic

也可以在终端上用下面的命令发送单条数据到指定Kafka
kafka-console-producer --broker-list localhost:9092 --topic ITMS_CHILD_PARENT_SCHEDULE

3.实时任务消费Kafka数据，验证计算逻辑

4.关闭zookeeper、Kafka
brew services stop zookeeper
brew services stop kafka
