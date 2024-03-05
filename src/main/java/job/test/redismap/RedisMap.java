package job.test.redismap;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import java.util.*;

public class RedisMap implements Map<String, Integer> {

    private final JedisPool jedisPool;
    private final String keyPrefix;

    public RedisMap(final JedisPool jedisPool, final String keyPrefix) {
        this.jedisPool = jedisPool;
        this.keyPrefix = keyPrefix;
    }

    private String getKey(final String key) {
        return keyPrefix + ":" + key;
    }

    @Override
    public int size() {
        try (final Jedis jedis = jedisPool.getResource()) {
            return Math.toIntExact(jedis.hlen(keyPrefix));
        } catch (JedisException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(final Object key) {
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists(keyPrefix, (String) key);
        } catch (JedisException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean containsValue(final Object value) {
        System.err.println("Unefficient operation RedisMap.containsValue()");
        try (final Jedis jedis = jedisPool.getResource()) {
            final Set<String> keys = jedis.hkeys(keyPrefix);
            for (String key : keys) {
                final String val = jedis.hget(keyPrefix, key);
                if (value != null && value.equals(Integer.parseInt(val))) {
                    return true;
                }
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (JedisException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Integer get(final Object key) {
        try (final Jedis jedis = jedisPool.getResource()) {
            final String value = jedis.hget(keyPrefix, (String) key);
            return value != null ? Integer.parseInt(value) : null;
        } catch (JedisException | NumberFormatException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Integer put(final String key, final Integer value) {
        try (final Jedis jedis = jedisPool.getResource()) {
            jedis.hset(keyPrefix, key, String.valueOf(value));
            return value;
        } catch (JedisException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Integer remove(final Object key) {
        try (final Jedis jedis = jedisPool.getResource()) {
            final String value = jedis.hget(keyPrefix, (String) key);
            jedis.hdel(keyPrefix, (String) key);
            return value != null ? Integer.parseInt(value) : null;
        } catch (JedisException | NumberFormatException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void putAll(final Map<? extends String, ? extends Integer> m) {
        try (final Jedis jedis = jedisPool.getResource()) {
            final Map<String, String> data = new HashMap<>();
            m.forEach((key, value) -> data.put(key, String.valueOf(value)));
            jedis.hmset(keyPrefix, data);
        } catch (JedisException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear() {
        try (final Jedis jedis = jedisPool.getResource()) {
            jedis.del(keyPrefix);
        } catch (JedisException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<String> keySet() {
        try (final Jedis jedis = jedisPool.getResource()) {
            return jedis.hkeys(keyPrefix);
        } catch (JedisException e) {
            e.printStackTrace();
            return Collections.emptySet();
        }
    }

    @Override
    public Collection<Integer> values() {
        System.err.println("Unefficient operation RedisMap.values()");
        try (final Jedis jedis = jedisPool.getResource()) {
            final List<Integer> values = new ArrayList<>();
            final Map<String, String> map = jedis.hgetAll(keyPrefix);
            for (String val : map.values()) {
                values.add(Integer.parseInt(val));
            }
            return values;
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (JedisException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public Set<Entry<String, Integer>> entrySet() {
        System.err.println("Unefficient operation RedisMap.entrySet()");
        final Set<Entry<String, Integer>> entrySet = new HashSet<>();
        try (final Jedis jedis = jedisPool.getResource()) {
            final Map<String, String> map = jedis.hgetAll(keyPrefix);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                entrySet.add(new AbstractMap.SimpleEntry<>(entry.getKey(), Integer.parseInt(entry.getValue())));
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (JedisException e) {
            e.printStackTrace();
        }
        return entrySet;
    }

}