package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/* 20000–30000 speiziell reserviert fuer slurm  */
final class PortManager {
    private static final int BASE = 20000;
    private static final int MAX = 30000;
    private static final Set<Integer> RESERVED = ConcurrentHashMap.newKeySet();

    /** get a free port for placeholder -> make it RESERVED */

    static int acquire() throws IOException {
        for (int p = BASE; p < MAX; p++) {
            if (!RESERVED.add(p)) continue;            // conflict
            if (isFree(p)) return p;                   // success
            RESERVED.remove(p);                        // bind failed
        }
        throw new IOException("No free port in pool");
    }

    static void release(int port) { RESERVED.remove(port); }

    private static boolean isFree(int port) {
        try (ServerSocket s = new ServerSocket()) {
            s.setReuseAddress(false);
            s.bind(new InetSocketAddress("0.0.0.0", port));
            return true;
        } catch (IOException e) { return false; }
    }

    // ich vergesse immer am ende close()anzurufen, benuztze autocloseable, anweisungen in {} verklammern
    static class PortReservation implements AutoCloseable {
        private final ServerSocket placeholder;
        final int port;

        PortReservation(int port) throws IOException {
            this.port = port;
            this.placeholder = new ServerSocket();
            this.placeholder.setReuseAddress(false);
            this.placeholder.bind(new InetSocketAddress("0.0.0.0", port));
        }
        @Override public void close() throws IOException { placeholder.close(); }
    }
// schickt ein paar

    static void waitUntilReachable(String host, int port, Duration timeout)
            throws IOException, InterruptedException {

        long deadline = System.nanoTime() + timeout.toNanos();
        long backoff  = 250; // ms!!!
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), 1000);
                return;
            } catch (IOException ignore) { }
            Thread.sleep(backoff);
            backoff = Math.min(backoff * 2, 2000);
        }
        throw new IOException("Port "+port+" did not open within "+timeout.toSeconds()+" s");
    }





}
