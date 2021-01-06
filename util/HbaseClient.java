package com.yd.module.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HbaseClient.class);
    static final Configuration conf = HBaseConfiguration.create();
    static final String tableName = "xxx";
    private static Connection conn = null;

    static {
        init();
    }


    private HbaseClient(){

    }

    private static class SingletonInstance{
        private static final HbaseClient INSTANCE = new HbaseClient();
    }

    public static HbaseClient getInstance(){
        return SingletonInstance.INSTANCE;
    }

    public static void init(){

        try{
            String quorum = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.quorum");
            String port = PropertiesLoader.getInstance().getProperty("hbase.zookeeper.port");
            conf.set("hbase.zookeeper.quorum", quorum);
            conf.set("zookeeper.znode.parent", "/hbase");
            conf.set("hbase.zookeeper.property.clientPort", port);
            conf.set("hbase.client.pause", "50");
            conf.set("hbase.client.retries.number", "3");
            conf.set("hbase.rpc.timeout", "2000");
            conf.set("hbase.client.operation.timeout", "3000");
            conf.set("hbase.client.scanner.timeout.period", "10000");
            conn = ConnectionFactory.createConnection(conf);
        }catch(Exception e){
            logger.info("初始化hbase连接失败:"+e);
        }
    }


    public Table getHtable() throws IOException {
        return conn.getTable(TableName.valueOf(tableName));
    }


    public void relaseHtable(Table table){
        if(table == null){
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            logger.info("hbase中表关闭失败:"+e);
        }
    }


    public void destory(){
        try {
            conn.close();
        } catch (IOException e) {
            logger.info("hbase中连接关闭失败:"+e);
        }
    }



}
