package org.pm.avro.test;

import example.avro.Message_map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.stat.StatUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author pmaresca
 */
public class AvroMapTest {

    private static final int TESTS = 1000;
    private static final int REPETITIONS = 1;
    private static final int RESCALE = 1000;
    private static final boolean DEBUG = false;
    //private static int[] PAYLOAD_SIZES = {64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768};
    private static int[] PAYLOAD_SIZES = {64};

    public static void main(String[] args) {
        Map<Integer, Double> timesSummary = new HashMap<Integer, Double>();
        Map<Integer, Double> sizesSummary = new HashMap<Integer, Double>();
        double[] times = new double[TESTS];
        double[] sizes = new double[TESTS];

        int cumulativeSize = 0;

        long startBenchmark = System.currentTimeMillis();
        try {
            AvroMapSerializer.instance()
                             .setUp();

            for (int s = 0; s < PAYLOAD_SIZES.length; s++) {
                for (int t = 0; t < TESTS; t++) {
                    long startTest = System.nanoTime();
                    for (int r = 0; r < REPETITIONS; r++) {
                        Collection<Message_map> messages = createMessages(1, PAYLOAD_SIZES[s]);

                        Iterator<Message_map> itr = messages.iterator();
                        while (itr.hasNext()) {
                            Message_map orgMsg = itr.next();
                            if (DEBUG)
                                System.out.println("Original:\n   " + orgMsg);
                            byte[] msgBytes = AvroMapSerializer.instance()
                                                               .serialize(orgMsg);
                            cumulativeSize += msgBytes.length;
                            Message_map rebMsg = (Message_map) AvroMapSerializer.instance()
                                                                                .deserialize(msgBytes);
                            if (DEBUG)
                                System.out.println("Rebuilt:\n   " + rebMsg);
                        }
                    }
                    times[t] = ((System.nanoTime() - startTest) / REPETITIONS);
                    sizes[t] = cumulativeSize / REPETITIONS;
                    cumulativeSize = 0;
                }
                timesSummary.put(PAYLOAD_SIZES[s], StatUtils.mean(times));
                sizesSummary.put(PAYLOAD_SIZES[s], StatUtils.mean(sizes));
            }

            AvroMapSerializer.instance()
                             .tearDown();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        printSummary(timesSummary, sizesSummary,
                (System.currentTimeMillis() - startBenchmark));
    }

    private static Collection<Message_map> createMessages(int nr, int size) {
        Collection<Message_map> messages = new LinkedList<>();
        Map<CharSequence, CharSequence> mapValue = new HashMap<CharSequence, CharSequence>() {{
            put("x", "y");
            //put("a", "b");
        }};

        for (int i = 0; i < nr; i++) {
            String payload = RandomStringUtils.randomAlphanumeric(size);
            Message_map msg = Message_map.newBuilder()
                                         .setMap(mapValue)
                                         .build();

            messages.add(msg);
        }

        return messages;
    }

    private static void printSummary(Map<Integer, Double> times,
                                     Map<Integer, Double> sizes,
                                     long timeSpent) {
        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append("Benchamrk Summary - Map type [");
        sBuilder.append(timeSpent);
        sBuilder.append("ms]\n");
        sBuilder.append("{");
        sBuilder.append("\n  #tests: ");
        sBuilder.append(TESTS);
        sBuilder.append("\n  #repetitions: ");
        sBuilder.append(REPETITIONS);
        sBuilder.append("\n");
        for (Integer key : times.keySet()) {
            sBuilder.append("\n\n  ");
            sBuilder.append(key.toString() + " bytes payload:");
            sBuilder.append("\n       mean_time[μs]: ");
            sBuilder.append(times.get(key) / RESCALE);
            sBuilder.append("\n    size_of_msg[bytes]: ");
            sBuilder.append(sizes.get(key));
        }
        sBuilder.append("\n}");

        System.out.println(sBuilder.toString());
    }

}
