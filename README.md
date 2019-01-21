# Spark
spark总结


Spark summary：


1.Spark rdd todo;



流处理Streaming：


streaming需要具备的特点：


1.低延迟;
2.一致性语义保证(end-to-end exactly-once);
3.吞吐量;
4.乱序数据;
5.持久化的状态存储.


Streaming发展简史：


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


趋势：
批量和流式编程模型统一，简单易用，流处理逐渐代替批处理

1.Spark Streaming  demo;


2.Structured Streaming demo;
