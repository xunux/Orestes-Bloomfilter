package orestes.bloomfilter.test;

import static orestes.bloomfilter.test.helper.Helper.createCountingFilter;
import static orestes.bloomfilter.test.helper.Helper.createCountingRedisFilter;
import static orestes.bloomfilter.test.helper.Helper.createRedisFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.test.helper.Helper;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BloomFilterTest {

    private final boolean redisBacked;
    private final boolean counting;
    private static final String name = "concurrencytests";

    @Parameterized.Parameters(name = "Bloom Filter test with {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {
                {"normal memory", false, false},
                {"counting memory", false, true},
                {"normal redis", true, false},
                {"counting redis", true, true}
        };
        return Arrays.asList(data);
    }

    private BloomFilter<String> createFilter(String name, int n, double p, HashMethod hm) {
        if (!redisBacked) {
            if (counting)
                return createCountingFilter(n, p, hm);
            else
                return Helper.createFilter(n, p, hm);
        } else {
            if (counting)
                return createCountingRedisFilter(name, n, p, hm);
            else
                return createRedisFilter(name, n, p, hm);
        }
    }

    public BloomFilterTest(String name, boolean redisBacked, boolean counting) {
        this.redisBacked = redisBacked;
        this.counting = counting;
    }

    @Ignore("Need to backport to Java7")
    @Test
    public void testMultiThreadedAddAndRemove() {
        ExecutorService exec = Executors.newFixedThreadPool(10);
        BloomFilter<String> filter = createFilter(name, 100_000, 0.001, HashMethod.Murmur2);
        int rounds = 100;
        List<String> inserted = new ArrayList<String>(rounds);
        for (int i = 0; i < rounds; i++) {
            inserted.add("obj" + String.valueOf(i));
        }
        // CompletableFuture[] futures = new CompletableFuture[rounds];
        // for (int i = 0; i < rounds; i++) {
        // String obj = inserted.get(i);
        // futures[i] = CompletableFuture.runAsync(() -> {
        // filter.add(obj);
        // assertTrue(filter.contains(obj));
        // if (counting) {
        // ((CountingBloomFilter<String>) filter).remove(obj);
        // boolean contained = filter.contains(obj);
        // assertFalse(contained);
        // filter.add(obj);
        // assertTrue(filter.contains(obj));
        // }
        // }, exec);
        // }
        // CompletableFuture.allOf(futures).join();
        List<String> notInserted = new ArrayList<String>(rounds);
        for (int i = rounds; i < rounds + 50; i++) {
            notInserted.add(String.valueOf(i));
        }

        for (String obj : inserted) {
            assertTrue(filter.contains(obj));
        }

        for (String obj : notInserted) {
            boolean found = filter.contains(obj);
            assertFalse(found);
        }
        assertTrue(filter.containsAll(inserted));
        filter.remove();
    }

    @Test
    public void normalTest() {
        int n = 26;
        double p = 0.01;
        BloomFilter<String> b = createFilter(name, n, p, HashMethod.MD5);
        b.add("Käsebrot");
        b.add("ist");
        b.add("ein");
        b.add("gutes");
        b.add("Brot");
        assertTrue(b.contains("Käsebrot"));
        assertTrue(b.contains("ist"));
        assertTrue(b.contains("ein"));
        assertTrue(b.contains("gutes"));
        assertTrue(b.contains("Brot"));
        assertTrue(!b.contains("Kartoffelsalat"));
        assertTrue(!b.contains("Dachlatte"));
        assertTrue(!b.contains("Die Secsdfhszehnte"));
        assertTrue(!b.contains("Die Saecsdfhszehnte"));
        assertTrue(!b.contains("Die Secsdfddhszehnte"));
        assertTrue(!b.contains("Die Secssdfhszehnte"));
        assertTrue(!b.contains("Die Sechszeaahnte"));
        assertTrue(!b.contains("Die Sechs34zehnte"));

    }

    @Test
    public void addAllTest() {
        int n = 100_000;
        double p = 0.01;
        int rounds = 100;
        BloomFilter<String> b1 = createFilter(name + "1", n, p, HashMethod.MD5);
        BloomFilter<String> b2 = createFilter(name + "2", n, p, HashMethod.MD5);
        BloomFilter<String> b3 = createFilter(name + "3", n, p, HashMethod.Murmur2);
        List<String> inserted = new ArrayList<String>(rounds);
        for (int i = 0; i < rounds; i++) {
            String s = "test" + String.valueOf(i);
            inserted.add(s);
            b1.add(s);
        }
        b2.addAll(inserted);
        b3.addAll(inserted);

        assertEquals(b1, b2);
        assertTrue(!b1.equals(b3) && !b2.equals(b3));
        assertFalse(b1.getBitSet().equals(b3.getBitSet()));
    }

    @Test
    public void differentHashFunctionsTest() {
        int n = 10_000;
        double p = 0.01;
        int rounds = 100;
        List<String> inserted = new ArrayList<String>(rounds);
        for (int i = 0; i < rounds; i++) {
            inserted.add("test" + String.valueOf(i));
        }
        List<String> notInserted = new ArrayList<String>(50);
        for (int i = rounds; i < rounds + 50; i++) {
            notInserted.add(String.valueOf(i));
        }
        for (HashMethod hm : HashMethod.values()) {
            BloomFilter<String> b = createFilter(name, n, p, hm);
            for (String s : inserted) {
                b.add(s);
            }

            for (String obj : inserted) {
                assertTrue(hm.toString() + " contains failed", b.contains(obj));
            }

            for (String obj : notInserted) {
                boolean found = b.contains(obj);
                assertFalse(hm.toString() + " not contains failed", found);
            }

            assertTrue(hm.toString() + " contains all failed", b.containsAll(inserted));
            b.remove();
        }
    }
}
