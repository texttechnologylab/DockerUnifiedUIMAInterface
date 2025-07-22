package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * he port numbers allowed here should be the same as ports:- xx:xx in docker-compose.yml.
 * Due to the algorithmic multiplexing of ports using fifo,
 * each component theoretically opens up a maximum of MAX-BASE+1 at the same time.
 * If there is not enough hardware to allocate, an error is reported.
 */
public class PortManager {
    private static final int BASE = 20000;
    private static final int MAX = 20100;
    private static final AtomicInteger next = new AtomicInteger(BASE);
    private static final ConcurrentLinkedQueue<Integer> reuse_queue = new ConcurrentLinkedQueue<>();

    public static int acquire() throws IOException {
        Integer fromPool = reuse_queue.poll();
        if (fromPool != null) return fromPool;

        int candidate = next.getAndIncrement();
        if (candidate >= MAX) {
            throw new IOException("No free port in pool (" + BASE + "‑" + (MAX) + ')');
        }
        return candidate;
    }

    public static void release(int port) {
        if (port < BASE || port >= MAX) return;
        reuse_queue.offer(port);
    }


    public static int remaining() {
        int produced = next.get() - BASE;
        int total = MAX - BASE;
        return total - produced + reuse_queue.size();
    }

    public static boolean isHttpReachable(String url,
                                          int expectedStatus,
                                          Predicate<String> bodyPredicate,
                                          int timeoutMillis) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(timeoutMillis);
            conn.setReadTimeout(timeoutMillis);
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            if (status != expectedStatus) {
                return false;
            }

            if (bodyPredicate == null) {
                return true;
            }

            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return bodyPredicate.test(sb.toString());
            }
        } catch (IOException e) {
            return false;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }


    public static boolean waitUntilHttpReachable(String url,
                                                 int expectedStatus,
                                                 Predicate<String> bodyPredicate,
                                                 Duration maxWait,
                                                 Duration interval) throws InterruptedException {
        Instant deadline = Instant.now().plus(maxWait);
        while (Instant.now().isBefore(deadline)) {
            if (isHttpReachable(url, expectedStatus, bodyPredicate,
                    (int) interval.toMillis())) {
                return true;
            }
            Thread.sleep(interval.toMillis());
        }
        return false;
    }

}


