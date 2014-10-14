package orestes.bloomfilter.test;

import java.util.Random;

import junit.framework.TestCase;
import orestes.bloomfilter.HashProvider;

import org.junit.Test;

import com.google.common.hash.Hashing;

public class MurmurTest {

    @Test
    public void testForEqualsHashes() {
        com.google.common.hash.HashFunction guavaHash = Hashing.murmur3_32();
        Random random = new Random();
        int maxBytes = 100;
        int maxTestsPerByte = 100;

        for (int i = 0; i < maxBytes; i++) {
            for (int j = 0; j < maxTestsPerByte; j++) {
                byte[] input = new byte[i];
                random.nextBytes(input);
                int theirs = guavaHash.hashBytes(input).asInt();
                int ours = HashProvider.murmur3(0, input);
                //System.out.println(i+","+j+":"+theirs+" | " + ours);
                TestCase.assertEquals(theirs, ours);
            }
        }
    }
}
