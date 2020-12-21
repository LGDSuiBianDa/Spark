package com.yd.spark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import java.util.Map;
import java.util.Set;

/**
 * @Description：redis分片连接池自动关闭连接工具类
 * @Author xxx
 * @CreateDate: 2020/12/21 15:51
 */
public class ShardRedisProxyUtils {

    private static final Logger logger = LoggerFactory.getLogger(ShardRedisProxyUtils.class);

    private static ShardedJedisSentinelPool shardPool = ShardedJedisSentinelPool.getInstance();

    private ShardRedisProxyUtils() {

    }

    public static Boolean exists(String key) {

        Jedis jedis = null;
        try {
            jedis = shardPool.getResource(key);
            return jedis.exists(key);
        } catch (Exception e) {
//            throw e;
            logger.error("redis hget error ! msg is : " ,e);
            return false;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Long del(String key) {

        Jedis jedis = null;
        try {
            jedis = shardPool.getResource(key);
            return jedis.del(key);
        } catch (Exception e) {
//            throw e;
            logger.error("redis del error ! msg is : " ,e);
            return -1L;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public static void set(String key, String value, int seconds) {

        Jedis jedis = null;
        try {
            jedis = shardPool.getResource(key);
            Pipeline pipeline = jedis.pipelined();
            pipeline.set(key, value);
            pipeline.expire(key, seconds);
            pipeline.sync();
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Long setnx(String key, String value) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.setnx(key, value);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Long expire(String key, int seconds) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static String get(String key) {

        Jedis jedis = null;
        try {
            jedis = shardPool.getResource(key);
            return jedis.get(key);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Boolean hexists(String key, String field) {

        Jedis jedis = null;
        try {
            jedis = shardPool.getResource(key);
            return jedis.hexists(key,field);
        } catch (Exception e) {
//            throw e;
            logger.error("redis hget error ! msg is : " ,e);
            return false;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static String hget(String key, String field) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.hget(key, field);
        } catch (Exception e) {
//            throw e;
            logger.error("redis hget error ! msg is : " ,e);
            return "NA";
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static void hset(String key, String field, String value,int seconds) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            Pipeline pipeline = jedis.pipelined();
            pipeline.hset(key, field, value);
            pipeline.expire(key, seconds);
            pipeline.sync();
        } catch (Exception e) {
//            throw e;
            logger.error("redis hset error ! msg is : " ,e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Map<String, String> hgetAll(String key) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.hgetAll(key);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static Long zadd(String key, double score, String menber) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.zadd(key, score, menber);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public static Set<Tuple> zrevrangeWithScores(String key, double min, double max) {
        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }


    public static Long hdel(String key, String field) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.hdel(key, field);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public static ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params) {

        Jedis jedis = null;
        try {

            jedis = shardPool.getResource(key);
            return jedis.hscan(key, cursor, params);
        } catch (Exception e) {
            throw e;
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    /**
     * 关闭redis连接池
     */
    public static void close() {

        try {
            if (shardPool != null) {
                shardPool.close();
                System.out.println("close the redis shard pool success!");
            }
        } catch (Exception e) {
            logger.error("close the redis shard pool error:", e);
        }
    }
}
