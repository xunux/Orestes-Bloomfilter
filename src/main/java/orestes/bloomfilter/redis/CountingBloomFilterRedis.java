package orestes.bloomfilter.redis;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.SafeEncoder;
import backport.java.util.function.Consumer;
import backport.java.util.function.Function;

/**
 * Uses regular key-value pairs for counting instead of a bitarray. This introduces a space overhead but allows
 * distribution of keys, thus increasing throughput. Pipelining can also be leveraged in this approach to minimize
 * network latency.
 * 
 * @param <T>
 */
public class CountingBloomFilterRedis<T> extends CountingBloomFilter<T> {
    private final RedisKeys keys;
    private final RedisPool pool;
    private final RedisBitSet bloom;
    private final FilterBuilder config;
    private final Long expireAt;

    public CountingBloomFilterRedis(FilterBuilder builder) {
        builder.complete();
        this.keys = new RedisKeys(builder.name());
        this.pool = builder.redisPool() == null ? new RedisPool(builder.redisHost(), builder.redisPort(), builder.redisConnections(), builder.getReadSlaves()) : builder.redisPool();
        this.bloom = new RedisBitSet(pool, keys.BITS_KEY, builder.size());
        this.config = keys.persistConfig(pool, builder);
        this.expireAt = builder.redisExpireAt();
        if(builder.overwriteIfExists())
            this.clear();
    }

    @Override
    public long addAndEstimateCount(final byte[] element) {
        List<Object> results = pool.transactionallyRetry(new Consumer<Pipeline>() {
            @Override
            public void accept(Pipeline p) {

                int[] hashes = hash(element);
                for (int position : hashes) {
                    bloom.set(p, position, true);
                }
                for (int position : hashes) {
                    p.hincrBy(keys.COUNTS_KEY, encode(position), 1);
                }

                setExpireAt(p);
            }
        }, keys.BITS_KEY, keys.COUNTS_KEY);

        long min = Long.MAX_VALUE;
        int n = 0, skip = config().hashes();
        for (Object object : results) {
            if (n < skip) {
                n++;
                continue;
            }
            Long l = (Long) object;
            min = (min >= l ? l : min);
        }

        return min;
    }

    // TODO removeALL addAll

    @Override
    public boolean remove(byte[] value) {
        return removeAndEstimateCount(value) <= 0;
    }

    @Override
    public long removeAndEstimateCount(final byte[] value) {
        return pool.safelyReturn(new Function<Jedis, Long>() {
            @Override
            public Long apply(Jedis jedis) {
                int[] hashes = hash(value);
                String[] hashesString = encode(hashes);

                Pipeline p = jedis.pipelined();
                p.watch(keys.COUNTS_KEY, keys.BITS_KEY);

                List<Long> counts;
                List<Response<Long>> responses = new ArrayList<>(config().hashes());
                for (String position : hashesString) {
                    responses.add(p.hincrBy(keys.COUNTS_KEY, position, -1));
                }
                setExpireAt(p);
                p.sync();

                counts = new ArrayList<Long>(responses.size());
                for (Response<Long> r : responses) {
                    counts.add(r.get());
                }

                while (true) {
                    p = jedis.pipelined();
                    p.multi();
                    for (int i = 0; i < config().hashes(); i++) {
                        if (counts.get(i) <= 0) bloom.set(p, hashes[i], false);
                    }
                    Response<List<Object>> exec = p.exec();
                    p.sync();
                    if (exec.get() == null) {
                        p = jedis.pipelined();
                        p.watch(keys.COUNTS_KEY, keys.BITS_KEY);
                        Response<List<String>> hmget = p.hmget(keys.COUNTS_KEY, hashesString);
                        p.sync();
                        counts = new ArrayList<Long>(responses.size());
                        for (String s : hmget.get()) {
                            counts.add(Long.valueOf(s));
                        }
                    } else {
                        return Collections.min(counts);
                    }
                }
            }
        });
    }
    
    public void setExpireAt(Pipeline p) {
        if(expireAt != null) {
            p.expireAt(keys.COUNTS_KEY, expireAt);
        }
    }

    @Override
    public long getEstimatedCount(final T element) {
        return pool.allowingSlaves().safelyReturn(new Function<Jedis, Long>() {
            @Override
            public Long apply(Jedis jedis) {
                String[] hashesString = encode(hash(toBytes(element)));
                List<String> hmget = jedis.hmget(keys.COUNTS_KEY, hashesString);

                Long min = null;
                for (String s : hmget) {
                    if(s == null) continue;
                    Long l = Long.valueOf(s);
                    if (min == null || l < min) min = l;
                }

                return min == null ? 0 : min;
            }
        });
    }

    @Override
    public void clear() {
        pool.safelyDo(new Consumer<Jedis>() {
            @Override
            public void accept(Jedis jedis) {
                jedis.del(keys.COUNTS_KEY, keys.BITS_KEY);
            }
        });
    }

    @Override
    public void remove() {
        clear();
        pool.safelyDo(new Consumer<Jedis>() {
            @Override
            public void accept(Jedis jedis) {
                jedis.del(config().name());
            }
        });
        pool.destroy();
    }

    @Override
    public boolean contains(byte[] element) {
        return bloom.isAllSet(hash(element));
    }

    protected RedisBitSet getRedisBitSet() {
        return bloom;
    }

    @Override
    public BitSet getBitSet() {
        return bloom.asBitSet();
    }

    @Override
    public FilterBuilder config() {
        return config;
    }

    public CountingBloomFilterMemory<T> toMemoryFilter() {
        CountingBloomFilterMemory<T> filter = new CountingBloomFilterMemory<>(config().clone());
        filter.getBitSet().or(getBitSet());
        return filter;
    }

    @Override
    public CountingBloomFilter<T> clone() {
        return new CountingBloomFilterRedis<>(config().clone());
    }

    @Override
    public boolean union(BloomFilter<T> other) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean intersect(BloomFilter<T> other) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return bloom.isEmpty();
    }

    @Override
    public Double getEstimatedPopulation() {
        return BloomFilter.population(bloom, config());
    }

    private static String encode(int value) {
        return SafeEncoder.encode(new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value});
    }

    private static String[] encode(int[] hashes) {
        String[] encoded = new String[hashes.length];
        for (int i = 0; i < hashes.length; i++) {
            encoded[i] = CountingBloomFilterRedis.encode(hashes[i]);
        }
        return encoded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CountingBloomFilterRedis)) return false;

        CountingBloomFilterRedis that = (CountingBloomFilterRedis) o;

        if (bloom != null ? !bloom.equals(that.bloom) : that.bloom != null) return false;
        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) return false;
        // TODO also checks counters

        return true;
    }

}
