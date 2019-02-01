# Spark
# 本地搭建spark开发环境(主要包含以下六步)：


#1.下载安装JDK1.8并配置环境变量(%JAVA_HOME%\bin 和 %JAVA_HOME%\jre\bin)；


#2.下载安装scala-2.12(版本跟spark一致)并配置环境变量(%SCALA_HOME%\bin)；


#3.下载安装hadoop(版本跟spark一致，这里选择hadoop-2.7.6.tar.gz)，解压到某个文件夹，然后配置环境变量(%HADOOP_HOME% 和 %HADOOP_HOME%\bin)，为了防止运行程序的时候出现nullpoint异常，下载hadoop.dll和winutils.exe, 然后复制到所安装hadoop的bin目录下；


#4.官网下载spark对应版本的安装包(这里选择spark-2.3.0-bin-hadoop2.7.tgz)，解压到某个文件夹(如F:\spark)，然后配置环境变量(F:\spark\spark-2.3.0-bin-hadoop2.7\bin)，打开cmd，运行spark-shell正常启动spark，则spark安装完毕；


#5.下载安装maven(apache-maven-3.5.4-bin.zip)，解压到某个文件夹，然后配置环境变量(%MAVEN_HOME%\bin)，cmd下用mvn -v查看Maven的版本信息


#6.下载安装IDEA，首次启动之后需要先安装scala插件。安装完成后重启IDEA，否则无法配置全局scala SDK。配置好SDK(1.8)和global library(scala-sdk-2.11.12)之后就可以创建一个Maven项目，然后打包(mvn package打包)上传到集群上，提交任务(spark-submit)，你的代码就可以在集群上运行了，通过spark UI可以查看执行情况。
最后附上pom文件配置：

#<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.yd.spark</groupId>
    <artifactId>dynamic-resource-schedule</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <!-- 指定Maven用什么编码来读取源码及文档  -->
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <!-- 指定Maven用什么编码来呈现站点的HTML文件  -->
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <org.slf4j.version>1.6.6</org.slf4j.version>
        <protostuff.version>1.0.5</protostuff.version>
    </properties>

    <build>
        <pluginManagement>
            <plugins>
                <!-- 编译scala的插件 -->
                <plugin>
                    <groupId>net.alchim31.maven</groupId>
                    <artifactId>scala-maven-plugin</artifactId>
                    <version>3.4.2</version>
                </plugin>
                <!-- 编译java的插件 -->
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.7.0</version>
                </plugin>
            </plugins>
        </pluginManagement>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <executions>
                    <execution>
                        <id>scala-compile-first</id>
                        <phase>process-resources</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>scala-test-compile</id>
                        <phase>process-test-resources</phase>
                        <goals>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <executions>
                    <execution>
                        <phase>compile</phase>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <!-- 打包插件 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.1.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

    
    <dependencies>
        <!-- https://mvnrepository.com/artifact/org.scala-lang/scala-library -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.12.8</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-compiler</artifactId>
            <version>2.12.8</version>
            <scope>compile</scope>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.2.0</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.2.0</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.2.0</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.avro</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.scala-lang</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
            <version>2.2.0</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.spark</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.apache.zookeeper</groupId>
                    <artifactId>zookeeper</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.scala-lang</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.apache.avro</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.11</artifactId>
            <version>0.8.2.2</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>org.scala-lang</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.avro/avro-tools -->
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro-tools</artifactId>
            <version>1.8.2</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.apache.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <dependency>
            <groupId>com.twitter</groupId>
            <artifactId>bijection-avro_2.11</artifactId>
            <version>0.9.6</version>
            <exclusions>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>



        <!-- https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop -->
        <!--dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-hadoop</artifactId>
            <version>6.2.4</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.elasticsearch.spark</groupId>
                </exclusion>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.elasticsearch.storm</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency-->
        <!-- https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20 -->
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-20_2.11</artifactId>
            <version>6.2.4</version>
            <exclusions>
                <exclusion>
                    <artifactId>*</artifactId>
                    <groupId>org.elasticsearch.hadoop</groupId>
                </exclusion>
                <exclusion>
                    <groupId>org.slf4j</groupId>
                    <artifactId>*</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>log4j</groupId>
                    <artifactId>log4j</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.54</version>
        </dependency>


    </dependencies>

</project>



# # # Spark summary：




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
