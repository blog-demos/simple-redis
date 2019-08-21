package pers.hai.simple.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.InputStream;
import java.util.Properties;

/**
 * Redis工具类
 *
 * Create Time: 2019-08-21 10:42
 * Last Modify: 2019-08-21
 *
 * @author Q-WHai
 * @see <a href="https://github.com/qwhai">https://github.com/qwhai</a>
 */
public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        // 加载配置文件
        InputStream input = RedisUtil.class.getClassLoader().getResourceAsStream("redis.properties");
        Properties properties = new Properties();

        try {
            properties.load(input);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // 获得池子对象
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(Integer.parseInt(properties.get("redis.maxIdle").toString()));    // 最大闲置数
        config.setMinIdle(Integer.parseInt(properties.get("redis.minIdle").toString()));    // 最小闲置数
        config.setMaxTotal(Integer.parseInt(properties.get("redis.maxTotal").toString()));  // 最大连接数

        jedisPool = new JedisPool(config, properties.getProperty("redis.url"), Integer.parseInt(properties.get("redis.port").toString()));
    }

    /**
     * 获取 Jedis 实例
     */
    public synchronized static Jedis getJedis() {
        try {
            if (null != jedisPool) {
                return jedisPool.getResource();
            } else {
                return null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /**
     * 释放资源
     */
    public static void returnResource(final Jedis jedis) {
        if (null == jedis)
            return;

        jedisPool.returnResource(jedis);
    }
}
