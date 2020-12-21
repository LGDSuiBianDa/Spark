package com.yd.spark.util;


import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Hashing;
import java.util.*;

/**
 * 用一致性哈希算法
 */
public class ShardedJedisSentinelPool {

    private static Map<String, JedisSentinelPool> poolMap = new HashMap<>();
    private static Hashing algo = Hashing.MURMUR_HASH;
    private static TreeMap<Long, String> nodes = new TreeMap<>();

    static {
        initPool();
    }
    private ShardedJedisSentinelPool(){

    }

    private static class SingletonInstance {

        private static final ShardedJedisSentinelPool INSTANCE = new ShardedJedisSentinelPool();
    }

    public static ShardedJedisSentinelPool getInstance() {

        return SingletonInstance.INSTANCE;
    }

    public static void initPool() {

        String[] masterNames = PropertiesLoader.getInstance().getProperty("redis.master.names").split(",");
        initialize(masterNames);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(Integer.parseInt(PropertiesLoader.getInstance().getProperty("redis.maxTotal")));// 最大连接数
        poolConfig.setMaxIdle(Integer.parseInt(PropertiesLoader.getInstance().getProperty("redis.maxIdle")));// 最大空闲数
        poolConfig.setMaxWaitMillis(Integer.parseInt(PropertiesLoader.getInstance().getProperty("redis.maxWait")));// 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：Could not get a resource from the pool
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        String[] hostAndPortArray = PropertiesLoader.getInstance().getProperty("redis.cluster").split(",");
        Set<String> sentinels = new HashSet<>();
        for (int i = 0; i < hostAndPortArray.length; i++) {
            String host = hostAndPortArray[i].split(":")[0];
            int port = Integer.parseInt(hostAndPortArray[i].split(":")[1]);
            sentinels.add(new HostAndPort(host, port).toString());
        }

        String password = PropertiesLoader.getInstance().getProperty("redis.auth");
        for (String masterName : masterNames) {
            poolMap.put(masterName, new JedisSentinelPool(masterName, sentinels, poolConfig, password));
        }
        System.out.println("the redis shard cluster pool init success");
    }

    public static JedisSentinelPool getShardPool(String key) {

        String masterName = getShardInfo(key.getBytes());
//        if (System.currentTimeMillis() % 500 == 1) {
//            System.out.println(key + "=" + masterName);
//        }
        return poolMap.get(masterName);
    }

    public static Jedis getResource(String key) {

        return getShardPool(key).getResource();
    }

    private static void initialize(String[] masterNames) {

        for (int i = 0; i != masterNames.length; ++i) {
            for (int n = 0; n < 160; n++) {
                nodes.put(algo.hash("SHARD-" + i + "-NODE-" + n), masterNames[i]);
            }
        }
    }

    public static String getShardInfo(byte[] key) {

        SortedMap<Long, String> tail = nodes.tailMap(algo.hash(key));
        if (tail.isEmpty()) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey());
    }

    public static void close() {
        for (Map.Entry<String, JedisSentinelPool> iter : poolMap.entrySet()) {
            iter.getValue().destroy();
        }
    }
}
