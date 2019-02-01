# streaming流处理的一般过程包括以下几步：
1.实时数据源接入（一般来源是kafka，本地可以用socket nc  测试小工具发送数据到spark streaming,来验证计算逻辑准确性）


2.探索性数据分析（一般来说如果你清楚地知道当前已有数据的结构，那么后续的分析或计算是水到渠成的；但是如果不知道当前已有数据的结构，就需要进行探索性数据分析）
no matter train a model or build a streaming task , EDA (Exploratory Data Analysis) is one of the basic and necessary step among the streaming pipeline.


3.业务逻辑（这一步包含了数据清洗、转换以及具体的计算逻辑，当然也可以是更加复杂的模型）


4.结果存储（将计算得到的结果持久化）


5.准确性验证（如果第三步包含了模型，则对应模型调优及评价）


# 开发一个streaming任务不是什么特别困难的事情，如何保证你的streaming任务结果准确，并且可以7*24不间断提供服务，这无疑是比较有挑战的：
1.结果准确性可以通过如下方法验证：
windows环境下的socket nc  测试小工具nc -L -p 9999  在windows本地 进行测试，发送socket包到spark streaming.
执行 nc -L -p 9999 -v ,手工输入数据,或者导入一个文件(nc -L -p 9999 -v < G:\test_data\part-00000).


2.7*24不间断提供服务通过checkpoint来异常恢复
