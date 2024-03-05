package job.test.redismap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Starter {


    public static void main(String[] args) {

        String redisHost = "localhost";
        int redisPort = 6379;

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        RedisMap map = new RedisMap(jedisPool, "prefix");
        map.put("key1", 1);
        map.put("key2", 2);

        System.out.println(map.get("key1"));
        System.out.println(map.get("key2"));
        System.out.println(map.containsKey("noKey"));
        System.out.println(map.containsKey("key1"));
        System.out.println(map.containsValue(1));
        System.out.println(map.containsValue(0));
        System.out.println(map.keySet());
        System.out.printf(map.entrySet().toString());
        System.out.println(map.size());
        System.out.println(map.remove("key1"));
        System.out.println(map.entrySet());
        System.out.println(map.values());
        System.out.println(map.isEmpty());

        jedisPool.close();
    }
}
