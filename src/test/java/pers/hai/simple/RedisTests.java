package pers.hai.simple;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.hai.simple.redis.RedisUtil;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Create Time: 2019-08-21 10:50
 * Last Modify: 2019-08-21
 *
 * @author Q-WHai
 * @see <a href="https://github.com/qwhai">https://github.com/qwhai</a>
 */
public class RedisTests {

    private Jedis jedis;
    private Logger logger = LoggerFactory.getLogger(RedisTests.class);

    /* --------------------- connect redis --------------------- */

    /**
     * 连接redis服务器
     */
    @Before
    public void connectRedis() {
        logger.info("ready to connect redis...");

        //jedis = new Jedis("192.168.37.152");
        jedis = RedisUtil.getJedis();
        if (null == jedis)
            return;
        jedis.auth("123456");

        logger.info("connected redis and successful.");
    }

    @After
    public void release() {
        logger.info("ready to release redis...");

        if (null == jedis)
            return;
        RedisUtil.returnResource(jedis);

        logger.info("released redis.");
    }

    /* --------------------- key --------------------- */

    /**
     * 在 key 存在时删除 key
     */
    @Test
    public void testDelKey() {
        jedis.set("key2", "adsfedfa");
        jedis.del("key2");
        Assert.assertNull(jedis.get("key2"));
    }

    /**
     * 检查给定 key 是否存在
     */
    @Test
    public void testExistsKey() {
        boolean b = jedis.exists("key2");
        Assert.assertTrue(b);
    }

    /* --------------------- string --------------------- */

    /**
     * 操作字符串
     */
    @Test
    public void testSetString() {
        jedis.set("name", "Hampton");
        Assert.assertEquals("Hampton", jedis.get("name"));
    }

    /**
     * 字符串拼接
     */
    @Test
    public void testAppendString() {
        jedis.del("key1"); // 先清空
        jedis.append("key1", "hello");
        jedis.append("key1", " ");
        jedis.append("key1", "world");
        Assert.assertEquals("hello world", jedis.get("key1"));
    }

    /**
     * 测试添加多个键值对。
     * 下标偶数为key，奇数为value
     */
    @Test
    public void testMset() {
        jedis.mset("name", "bob", "age", "18");
        Assert.assertEquals("bob", jedis.get("name"));
        Assert.assertEquals("18", jedis.get("age"));
    }

    /**
     * 测试针对数值型数据的自增处理
     */
    @Test
    public void testIncr() {
        jedis.set("key2", "10");
        jedis.incr("key2");
        Assert.assertEquals("11", jedis.get("key2"));
    }

    /* --------------------- set --------------------- */

    @Test
    public void testAdd2Set() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        Assert.assertEquals(3L, (long)jedis.scard("set1"));
        Set<String> members = jedis.smembers("set1");
        logger.info(String.format("Members: %s", members));
    }

    /**
     * 多集合的交集
     */
    @Test
    public void testMulitSet() {
        jedis.del("set1", "set2");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set2", "dsfe");
        jedis.sadd("set2", "fre3241");

        Set<String> members = jedis.sinter("set1", "set2");
        logger.info(String.format("Members: %s", members));
    }

    @Test
    public void testSinterstore() {
        jedis.del("set1", "set2", "set3");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set2", "dsfe");
        jedis.sadd("set2", "fre3241");

        Long x = jedis.sinterstore("set3", "set1", "set2");
        logger.info(String.format("x: %s", x));

        Set<String> members = jedis.smembers("set3");
        logger.info(String.format("Members: %s", members));
    }

    @Test
    public void testSismember() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        Assert.assertTrue(jedis.sismember("set1", "feaef"));
    }

    @Test
    public void testSmove() {
        jedis.del("set1", "set2");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set2", "fre3241");

        logger.info(String.format("[0] MEMBERS: %s", jedis.smembers("set1")));
        logger.info(String.format("[0] MEMBERS: %s", jedis.smembers("set2")));

        Long a = jedis.smove("set1", "set2", "feaef"); // 移动一个存在的元素
        logger.info(String.format("STATUS: %s", a));
        logger.info(String.format("[1] MEMBERS: %s", jedis.smembers("set1")));
        logger.info(String.format("[1] MEMBERS: %s", jedis.smembers("set2")));

        Long b = jedis.smove("set2", "set1", "abc"); // 移动一个不存在的元素
        logger.info(String.format("STATUS: %s", b));
        logger.info(String.format("[2] MEMBERS: %s", jedis.smembers("set1")));
        logger.info(String.format("[2] MEMBERS: %s", jedis.smembers("set2")));
    }

    @Test
    public void testStop1() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        logger.info(String.format("[0] MEMBERS: %s", jedis.smembers("set1")));
        logger.info(String.format("%s", jedis.spop("set1")));
        logger.info(String.format("[1] MEMBERS: %s", jedis.smembers("set1")));
    }

    @Test
    public void testStop2() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        logger.info(String.format("[0] MEMBERS: %s", jedis.smembers("set1")));
        logger.info(String.format("%s", jedis.spop("set1", 2)));
        logger.info(String.format("[1] MEMBERS: %s", jedis.smembers("set1")));
    }

    @Test
    public void testSrandmember() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        String v1 = jedis.srandmember("set1");
        logger.info(v1);

        List<String> v2 = jedis.srandmember("set1", 2);
        logger.info(String.format("v2: %s", v2));
    }

    @Test
    public void testSrem() {
        jedis.del("set1");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set1", "fre3241");

        logger.info(String.format("[0] MEMBERS: %s", jedis.smembers("set1")));
        jedis.srem("set1", "dsfe", "fre3241");
        logger.info(String.format("[1] MEMBERS: %s", jedis.smembers("set1")));
    }

    @Test
    public void testSunion() {
        jedis.del("set1", "set2");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set2", "fre3241");
        jedis.sadd("set2", "dsfe");

        Set<String> set3 = jedis.sunion("set1", "set2");
        logger.info(String.format("MEMBERS: %s", set3));
    }

    @Test
    public void testSunionstore() {
        jedis.del("set1", "set2", "set3");

        jedis.sadd("set1", "feaef");
        jedis.sadd("set1", "dsfe");
        jedis.sadd("set2", "fre3241");
        jedis.sadd("set2", "dsfe");

        jedis.sunionstore("set3", "set1", "set2");
        logger.info(String.format("MEMBERS: %s", jedis.smembers("set3")));
    }

    @Test
    public void testSscan() {
        jedis.del("set1");

        jedis.sadd("set1", "0");
        jedis.sadd("set1", "1");
        jedis.sadd("set1", "2");
        jedis.sadd("set1", "3");
        jedis.sadd("set1", "4");
        jedis.sadd("set1", "5");
        jedis.sadd("set1", "6");
        jedis.sadd("set1", "7");
        jedis.sadd("set1", "8");
        jedis.sadd("set1", "9");
        jedis.sadd("set1", "10");
        jedis.sadd("set1", "11");
        jedis.sadd("set1", "12");
        jedis.sadd("set1", "13");
        jedis.sadd("set1", "14");
        jedis.sadd("set1", "15");
        jedis.sadd("set1", "16");
        jedis.sadd("set1", "17");
        jedis.sadd("set1", "18");
        jedis.sadd("set1", "19");
        jedis.sadd("set1", "20");
        jedis.sadd("set1", "21");
        jedis.sadd("set1", "22");
        jedis.sadd("set1", "23");
        jedis.sadd("set1", "24");
        jedis.sadd("set1", "25");
        jedis.sadd("set1", "26");
        jedis.sadd("set1", "27");
        jedis.sadd("set1", "28");
        jedis.sadd("set1", "29");
        jedis.sadd("set1", "30");
        jedis.sadd("set1", "31");
        jedis.sadd("set1", "32");
        jedis.sadd("set1", "33");
        jedis.sadd("set1", "34");
        jedis.sadd("set1", "35");
        jedis.sadd("set1", "36");
        jedis.sadd("set1", "37");
        jedis.sadd("set1", "38");
        jedis.sadd("set1", "39");

        ScanResult<String> sr = jedis.sscan("set1", "1");
        logger.info(String.format("RESULT: %s", sr.getResult()));
    }

    /* --------------------- map --------------------- */

    /**
     * 向 Hash 中添加数据
     */
    @Test
    public void testAdd2Hash() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("name", "bob");
            put("age", "18");
            put("sno", "1000001");
        }};

        jedis.hmset("student", map);

        logger.info(String.format("ALL KEYS: %s", jedis.hkeys("student")));
        logger.info(String.format("ALL VALUES: %s", jedis.hvals("student")));

        List<String> rsmap = jedis.hmget("student", "sno", "name"); // 获取所有给定字段的值
        logger.info(String.format("Hash Values: %s", rsmap));
    }

    /**
     * 针对 Hash 进行删除操作
     */
    @Test
    public void testDelFromHash() {
        Map<String, String> map = new HashMap<String, String>(){{
            put("name", "bob");
            put("age", "18");
            put("sno", "1000001");
        }};

        jedis.hmset("student", map);

        jedis.hdel("student", "age");
        Assert.assertNull(jedis.hmget("student", "age").get(0)); // 这里要注意的是直接获取的 List<Stirng> 是不为空的，需要进行 .get(0) 获取其中的元素。因为删除，所以元素为空
        Assert.assertEquals(2L, (long)jedis.hlen("student")); // 获取哈希表中字段的数量
    }

    /* --------------------- list --------------------- */

    /**
     * 向列表中添加元素
     */
    @Test
    public void testAdd2List() {
        jedis.del("list_1");

        // 从左边添加数据
        jedis.lpush("list_1", "1");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "3");
        jedis.lpush("list_1", "4");
        jedis.lpush("list_1", "5");
        jedis.lpush("list_1", "5");

        // 从右边添加数据
        jedis.rpush("list_1", "6");
        jedis.rpush("list_1", "7");
        jedis.rpush("list_1", "8");
        jedis.rpush("list_1", "9");
        jedis.rpush("list_1", "10");

        List<String> result = jedis.lrange("list_1", 0, -1); // -1 表示取出所有数据
        logger.info(String.format("LIST RESULT: %s", result));

        Assert.assertEquals(11L, (long)jedis.llen("list_1"));
    }

    @Test
    public void testPopFromList() {
        jedis.del("list_1");

        jedis.lpush("list_1", "1");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "3");
        jedis.lpush("list_1", "4");
        jedis.lpush("list_1", "5");

        String val = jedis.lpop("list_1"); // 移出并获取列表的第一个元素
        Assert.assertEquals("5", val);

        val = jedis.rpop("list_1"); // 移出并获取列表的最后一个元素
        Assert.assertEquals("1", val);
    }

    @Test
    public void testBPopFromList() {
        jedis.del("list_1");

        List<String> val = jedis.blpop(3, "list_1"); // 从左移出元素。timeout 单位：秒
        System.out.println(val);

        val = jedis.brpop(3, "list_1"); // 从右移出元素。timeout 单位：秒
        System.out.println(val);

        jedis.lpush("list_1", "1");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "3");
        jedis.lpush("list_1", "4");
        jedis.lpush("list_1", "5");

        val = jedis.blpop(3, "list_1"); // timeout 单位：秒
        System.out.println(val);

        val = jedis.brpop(3, "list_1"); // timeout 单位：秒
        System.out.println(val);
    }

    @Test
    public void testGetFromList() {
        jedis.del("list_1");

        jedis.lpush("list_1", "1");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "3");
        jedis.lpush("list_1", "4");
        jedis.lpush("list_1", "5");

        String val = jedis.lindex("list_1", 3);
        Assert.assertEquals("2", val);
    }

    /**
     * 测试插入操作
     */
    @Test
    public void testListInsert() {
        jedis.del("list_1");

        jedis.lpush("list_1", "1");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "3");
        jedis.lpush("list_1", "2");
        jedis.lpush("list_1", "4");
        jedis.lpush("list_1", "5");

        jedis.linsert("list_1", BinaryClient.LIST_POSITION.BEFORE, "2", "7");

        List<String> result = jedis.lrange("list_1", 0, -1); // -1 表示取出所有数据
        logger.info(String.format("LIST RESULT: %s", result));
    }

    /* --------------------- sorted set --------------------- */

    @Test
    public void testZadd() {
        jedis.del("zset1");

        jedis.zadd("zset1", 1, "a");
        jedis.zadd("zset1", 3, "c");
        jedis.zadd("zset1", 2, "b");
        jedis.zadd("zset1", 5, "d");
        jedis.zadd("zset1", 4, "e");

        Set<String> set = jedis.zrange("zset1", 0, -1);
        logger.info(String.format("MEMBERS: %s", set));
        logger.info(String.format("LENGTH: %s", jedis.zcard("zset1")));
        logger.info(String.format("COUNT: %s", jedis.zcount("zset1", 3, 5)));
    }

    /* --------------------- pub/sub --------------------- */

    /**
     * 测试消息发布
     */
    @Test
    public void testPublish() {
        // 测试此发布者，需要在 Xshell 中先开启订阅者，并对 channel-1 进行监听
        jedis.publish("channel-1", "one message from java.");
    }

    /**
     * 测试消息订阅
     */
    @Test
    public void test() {
        // 这里是保持对 channel-2 通道的阻塞监听，可以通过在 Xshell 或是其他发布模块中进行消息发布
        jedis.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                logger.info(String.format("channel: %s, message: %s", channel, message));
            }
        }, "channel-2");
    }
}
