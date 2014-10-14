package orestes.bloomfilter.redis;

import java.util.BitSet;
import java.util.List;

import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;
import backport.java.util.function.Consumer;
import backport.java.util.function.Function;

/**
 * A persistent BitSet backed by Redis. Not all methods of the superclass are implemented. If needed they can be used
 * converting the RedisBitSet to a regular BitSet by calling {@link #asBitSet()}. <br> <br> External transactions or
 * pipeline can be propagated for use by modifying methods (e.g. {@link #set(int)}).
 */
public class RedisBitSet extends BitSet {
    private final RedisPool pool;
    private String name;
    private int size;

    /**
     * Constructs an new RedisBitSet.
     * 
     * @param name the name used as key in the database
     * @param size the initial size of the RedisBitSet
     */
    public RedisBitSet(RedisPool pool, String name, int size) {
        this.pool = pool;
        this.name = name;
        this.size = size;
    }

    @Override
    public boolean get(final int bitIndex) {
        return pool.allowingSlaves().safelyReturn(new Function<Jedis, Boolean>() {
            @Override
            public Boolean apply(Jedis jedis) {
                return jedis.getbit(name, bitIndex);
            }
        });
    }

    /**
     * Fetches the values at the given index positions in a multi transaction. This guarantees a consistent view.
     * 
     * @param indexes the index positions to query
     * @return an array containing the values at the given index positions
     */
    public Boolean[] getBulk(final int... indexes) {
        List<Boolean> results = pool.allowingSlaves().transactionallyDo(new Consumer<Pipeline>() {
            @Override
            public void accept(Pipeline p) {
                for (int index : indexes) {
                    get(p, index);
                }
            }
        });
        return results.toArray(new Boolean[indexes.length]);
    }

    @Override
    public void set(final int bitIndex, final boolean value) {
        pool.safelyDo(new Consumer<Jedis>() {
            @Override
            public void accept(Jedis jedis) {
                jedis.setbit(name, bitIndex, value);
            }
        });
    }

    public void get(Pipeline p, int position) {
        p.getbit(name, position);
    }

    /**
     * Performs the normal {@link #set(int, boolean)} operation using the given pipeline.
     *
     * @param p        the propagated pipeline
     * @param bitIndex a bit index
     * @param value    a boolean value to set
     */
    public void set(Pipeline p, int bitIndex, boolean value) {
        p.setbit(name, bitIndex, value);
    }

    @Override
    public void set(int bitIndex) {
        set(bitIndex, true);
    }

    @Override
    public void clear(int bitIndex) {
        set(bitIndex, false);
    }

    @Override
    public void clear() {
        pool.safelyDo(new Consumer<Jedis>() {
            @Override
            public void accept(Jedis jedis) {
                jedis.del(name);
            }
        });
    }

    @Override
    public int cardinality() {
        return pool.safelyReturn(new Function<Jedis, Long>() {
            @Override
            public Long apply(Jedis jedis) {
                return jedis.bitcount(name);
            }
        }).intValue();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public byte[] toByteArray() {
        return pool.allowingSlaves().safelyReturn(new Function<Jedis, byte[]>() {
            @Override
            public byte[] apply(Jedis jedis) {
                byte[] bytes = jedis.get(SafeEncoder.encode(name));
                if (bytes == null) {
                    // prevent null values
                    bytes = new byte[(int) Math.ceil(size / 8)];
                }
                return bytes;
            }
        });
    }

    /**
     * Returns the RedisBitSet as a regular BitSet.
     * 
     * @return this RedisBitSet as a regular BitSet
     */
    public BitSet asBitSet() {
        return fromByteArrayReverse(toByteArray());
    }

    /**
     * Overwrite the contents of this RedisBitSet by the given BitSet.
     * 
     * @param bits a regular BitSet used to overwrite this RedisBitSet
     */
    public void overwriteBitSet(final BitSet bits) {
        pool.safelyDo(new Consumer<Jedis>() {
            @Override
            public void accept(Jedis jedis) {
                jedis.set(SafeEncoder.encode(name), toByteArrayReverse(bits));
            }
        });
    }

    @Override
    public String toString() {
        return asBitSet().toString();
    }

    public String getRedisKey() {
        return name;
    }

    /**
     * Tests whether the provided bit positions are all set.
     * 
     * @param positions the positions to test
     * @return <tt>true</tt> if all positions are set
     */
    public boolean isAllSet(int... positions) {
        Boolean[] results = getBulk(positions);
        for (Boolean b : results) {
            if (!b) return false;
        }
        return true;
    }

    /**
     * Set all bits
     * 
     * @param positions
     * @return {@code true} if any of the bits was previously unset.
     */
    public boolean setAll(final int... positions) {
        List<Object> results = pool.transactionallyDo(new Consumer<Pipeline>() {
            @Override
            public void accept(Pipeline p) {
                for (int position : positions)
                    p.setbit(name, position, true);
            }
        });

        // anyMatch
        for (Object b : results) {
            if (!(Boolean) b) return true;
        }
        return false;
    }

    // Copied from: https://github.com/xetorthio/jedis/issues/301
    public static BitSet fromByteArrayReverse(final byte[] bytes) {
        final BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[i / 8] & (1 << (7 - (i % 8)))) != 0) {
                bits.set(i);
            }
        }
        return bits;
    }

    // Copied from: https://github.com/xetorthio/jedis/issues/301
    public static byte[] toByteArrayReverse(final BitSet bits) {
        final byte[] bytes = new byte[bits.length() / 8 + 1];
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                final int value = bytes[i / 8] | (1 << (7 - (i % 8)));
                bytes[i / 8] = (byte) value;
            }
        }
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RedisBitSet)
            obj = ((RedisBitSet) obj).asBitSet();
        return asBitSet().equals(obj);
    }
}
