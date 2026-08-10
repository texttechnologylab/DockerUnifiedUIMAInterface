package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.LocalPortForwarder;
import net.schmizz.sshj.connection.channel.direct.Parameters;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class SSHForwarder implements AutoCloseable {
    private SSHClient ssh;
    private LocalPortForwarder forwarder;
    private ServerSocket serverSocket;

    // ssh -N -f -L localhost:localPort:computeNode:computePort zhou@fuchs.hhlr-gu.de
    public void start( int localPort, String pwd, String user, String computeNode, int computePort, String sshHost) throws Exception {
        // connect to jumper
        ssh = new SSHClient();
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.connect(sshHost, 22);
        ssh.authPassword(user, pwd);

        Parameters params = new Parameters("127.0.0.1", localPort, computeNode, computePort);

        serverSocket = new ServerSocket();

        serverSocket.setReuseAddress(true);
        serverSocket.bind(new java.net.InetSocketAddress("127.0.0.1", localPort));
        forwarder = ssh.newLocalPortForwarder(params, serverSocket);

        // TODO make it daemon, dont forget destroy before jvm end, dopple check
        Thread daemon = new Thread(() -> {
            try {
                forwarder.listen();
            } catch (IOException ignored) {
            }
        });
        daemon.setDaemon(true);
        daemon.setName("DUUI-Tunnel-Daemon-" + localPort);
        daemon.start();

        System.out.printf("[SSH-Tunnel] Daemeon for Forwarding：localhost:%d ===> %s:%d\n", localPort, computeNode, computePort);
    }

    @Override
    public void close() {
        System.out.println("[SSH-Tunnel] kill forwarding...");
        try {

            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            if (forwarder != null) {
                forwarder.close(); //
            }
            if (ssh != null && ssh.isConnected()) {
                ssh.disconnect();
            }
        } catch (IOException ignored) {
        }
    }
}
