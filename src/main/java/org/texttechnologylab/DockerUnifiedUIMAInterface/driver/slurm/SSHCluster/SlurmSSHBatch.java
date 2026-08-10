package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster;

import com.jcraft.jsch.JSch;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

import net.schmizz.sshj.common.IOUtils;
//import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.util.regex.Pattern;

public interface SlurmSSHBatch {


    public static class SSHHelper {
        // all params i need to ssh connect server
        public final String host;
        public final String user;
        public final int port;
        public final String password;
        // keep alive
        private com.jcraft.jsch.Session session = null;
        private final JSch jsch;

        public SSHHelper(String host, String user, int port, String password) {
            this.host = host;
            this.user = user;
            this.port = port;
            this.password = password;
            this.jsch = new JSch();
        }
        // TODO sshj or jsch? jsch is maybe too old...
//
//
//        public boolean connect() {
//            try {
//                if (session != null && session.isConnected()) {
//                    return true;
//                }
//
//                session = jsch.getSession(user, host, port);
//                session.setPassword(password);
//
//                java.util.Properties config = new java.util.Properties();
//                config.put("StrictHostKeyChecking", "no");
//                session.setConfig(config);
//
//                System.out.println("[SSH] try connect " + host + "...");
//                session.connect(10000);
//
//                System.out.println("[SSH] Connected！");
//                return true;
//
//            } catch (Exception e) {
//                System.err.println("[SSH] Fail to connection: " + e.getMessage());
//                return false;
//            }
//        }
//
//
//        public void disconnectAll() {
//            if (session != null && session.isConnected()) {
//                session.disconnect();
//                System.out.println("[SSH] over and done.");
//            }
//        }
//
//
//        public String executeAndCapture(Session session, List<String> commands) {
//            if (commands == null || commands.isEmpty()) {
//                return "";
//            }
//
//            //
//            String command = commands.stream().reduce("true", (a, b) -> a + " && " + b);
//
//            if (session == null || !session.isConnected()) {
//                return "[SSH] ERROR, connection is not established";
//            }
//
//            StringBuilder outputResult = new StringBuilder();
//            StringBuilder errorResult = new StringBuilder();
//            ChannelExec channel = null;
//
//            try {
//                channel = (ChannelExec) session.openChannel("exec");
//                channel.setCommand(command);
//
//                InputStream stdOutRaw = channel.getInputStream();
//                InputStream stdErrRaw = channel.getExtInputStream();
//
//                channel.connect();
//
//                byte[] tmp = new byte[1024];
//                while (true) {
//                    // if data then read else no jam
//                    while (stdOutRaw.available() > 0) {
//                        int i = stdOutRaw.read(tmp, 0, 1024);
//                        if (i < 0) break;
//                        outputResult.append(new String(tmp, 0, i, StandardCharsets.UTF_8));
//                    }
//
//                    // 2. read both stream
//                    while (stdErrRaw.available() > 0) {
//                        int i = stdErrRaw.read(tmp, 0, 1024);
//                        if (i < 0) break;
//                        errorResult.append(new String(tmp, 0, i, StandardCharsets.UTF_8));
//                    }
//
//
//                    if (channel.isClosed()) {
//                        if (stdOutRaw.available() > 0 || stdErrRaw.available() > 0) {
//                            continue;
//                        }
//                        int exitStatus = channel.getExitStatus();
//                        if (exitStatus != 0) {
//                            System.err.println("[Alert] Exit failed " + exitStatus);
//                        }
//                        break;
//                    }
//                    Thread.sleep(45);
//                }
//
//                if (errorResult.length() > 0) {
//                    System.err.println("[ClusterError]: " + errorResult.toString().trim());
//                }
//
//            } catch (Exception e) {
//                System.err.println("error:");
//                e.printStackTrace();
//            } finally {
//                if (channel != null && channel.isConnected()) {
//                    channel.disconnect();
//                }
//            }
//
//            return outputResult.toString().trim();
//        }
//
//        public JSch getJsch() {
//            return jsch;
//        }
//
//        public Session getSession() {
//            return session;
//        }


        public static List<String> submitMultipleJobs(String host, String username, String password, List<String> sbatchCommands) throws Exception {
            if (sbatchCommands == null || sbatchCommands.isEmpty()) {
                return new ArrayList<>();
            }

            net.schmizz.sshj.SSHClient ssh = new net.schmizz.sshj.SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());

            List<String> jobIds = new ArrayList<>();

            try {
                System.out.printf("[SlurmDriver] Connect Cluster %s...\n", host);
                ssh.connect(host);
                ssh.authPassword(username, password);

                System.out.printf("[SlurmDriver] Submit %d Job...\n", sbatchCommands.size());
                //TODO make sure last one done and already get id
                for (int i = 0; i < sbatchCommands.size(); i++) {
                    String command = sbatchCommands.get(i);
                    try {
                        String jobId = executeSbatchAndExtractId(ssh, command);
                        jobIds.add(jobId);
                        System.out.println("[SlurmDriver] submit " + jobId);
                    } catch (Exception e) {
                        System.err.printf("[SlurmDriver] submit %d. job failed: %s. error: %s\n",
                                i + 1, command, e.getMessage());

                        throw e;
                    }
                }
                return jobIds;

            } finally {
                if (ssh.isConnected()) {
                    ssh.disconnect();
                }
            }
        }

        private static String executeSbatchAndExtractId(net.schmizz.sshj.SSHClient ssh, String sbatchCommand) throws IOException {
            try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(sbatchCommand);


                String stdout = IOUtils.readFully(cmd.getInputStream()).toString().trim();
                String stderr = IOUtils.readFully(cmd.getErrorStream()).toString().trim();

                cmd.join(10, TimeUnit.SECONDS);
                int exitStatus = cmd.getExitStatus();

                if (exitStatus != 0) {
                    throw new IOException("sbatch failed with exit code " + exitStatus + ". Error: " + stderr);
                }

                Pattern SBATCH_RESPONSE_PATTERN =
                        Pattern.compile("Submitted\\s+batch\\s+job\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
                Matcher matcher = SBATCH_RESPONSE_PATTERN.matcher(stdout);
                if (matcher.find()) {
                    return matcher.group(1);
                } else {
                    throw new IOException("Could not parse Job ID from sbatch stdout: " + stdout);
                }


            }

        }
        private static String queryNodeWithSshj(net.schmizz.sshj.SSHClient ssh, String jobId) throws IOException {
            try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                // TODO only use sacct, it can see history log
                String commandStr = "sacct -j " + jobId + " -X -o \"NodeList\" -n -P";
                net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(commandStr);

                String stdout = "";
                String stderr = "";
                try {
                    stdout = IOUtils.readFully(cmd.getInputStream()).toString().trim();
                    stderr = IOUtils.readFully(cmd.getErrorStream()).toString().trim();
                } catch (IOException e) {
                    System.err.println("[sshj] read sshj stream error: " + e.getMessage());
                }

                cmd.join(5, TimeUnit.SECONDS);

                int exitStatus = cmd.getExitStatus();
                if (exitStatus != 0) {
                    if (!stderr.isEmpty()) {
                        System.err.printf("[sshj] sacct failed (Exit Code: %d): %s\n", exitStatus, stderr);
                    }
                    return "";
                }

                if (stdout.isEmpty() || stdout.equalsIgnoreCase("None") || stdout.contains("expected") || stdout.contains(" ")) {
                    return "";
                }

                String[] lines = stdout.split("\\r?\\n");
                if (lines.length > 0) {
                    String candidateNode = lines[0].trim();
                    if (!candidateNode.isEmpty() && !candidateNode.equalsIgnoreCase("None")) {
                        return candidateNode;
                    }
                }

                return "";
            }
        }

        public static String waitForNodeAllocationSshj(String host, String username, String password, String jobId, int timeoutSeconds) throws Exception {
            net.schmizz.sshj.SSHClient ssh = new net.schmizz.sshj.SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());

            ssh.setConnectTimeout(10000);
            ssh.setTimeout(15000);
            System.out.printf("[sshj] ask for Job %s status...\n", jobId);
            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

            try {
                ssh.connect(host);
                ssh.authPassword(username, password);

                while (System.currentTimeMillis() < deadline) {

                    if (!ssh.isConnected() || !ssh.isAuthenticated()) {
                        System.out.println("[sshj] reconnecting...");
                        try {
                            ssh.disconnect();
                        } catch (Exception ignored) {}
                        ssh.connect(host);
                        ssh.authPassword(username, password);
                    }

                    String node = queryNodeWithSshj(ssh, jobId);
                    if (!node.isEmpty()) {
                        System.out.printf("[sshj] Job %s now running on [%s] \n", jobId, node);
                        return node;
                    }
                    System.out.printf("[sshj] Job %s (Pending/Waiting)，try later ...\n", jobId);
                    Thread.sleep(5000);
                }
                throw new RuntimeException("wait Job " + jobId + " failed，time is up and delay：" + timeoutSeconds + " seconds");
            } finally {
                if (ssh.isConnected()) {
                    ssh.disconnect();
                }
            }
        }



        public static String blockUntilRunning(String host, String username, String password, String jobId, int timeoutSeconds, AtomicBoolean shutdown) throws Exception {
            net.schmizz.sshj.SSHClient ssh = new net.schmizz.sshj.SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());

            ssh.setConnectTimeout(10000);
            ssh.setTimeout(15000);

            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);
            System.out.printf("[SlurmDriver]wait until resource allocated\n", jobId);

            try {
                ssh.connect(host);
                ssh.authPassword(username, password);

                while (System.currentTimeMillis() < deadline) {

                    if (shutdown != null && shutdown.get()) {
                        throw new InterruptedException("intterupt。");
                    }

                    if (!ssh.isConnected() || !ssh.isAuthenticated()) {
                        System.out.println("[SlurmDriver] SSH reconnecting...");
                        try { ssh.disconnect(); } catch (Exception ignored) {}
                        ssh.connect(host);
                        ssh.authPassword(username, password);
                    }

                    String squeueCmd = "squeue -j " + jobId + " -o \"%T %N\" -h";
                    String squeueStdout = "";

                    try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                        net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(squeueCmd);
                        squeueStdout = IOUtils.readFully(cmd.getInputStream()).toString().trim();
                        cmd.join(5, TimeUnit.SECONDS);

                        if (cmd.getExitStatus() == 0 && !squeueStdout.isEmpty()) {
                            String[] parts = squeueStdout.split("\\s+");
                            if (parts.length >= 1) {
                                String state = parts[0].toUpperCase();

                                // get node name
                                if ("RUNNING".equals(state) && parts.length >= 2) {
                                    String nodeName = parts[1].trim();
                                    System.out.printf("[SlurmDriver] Resource allocated！Job %s on [%s] \n", jobId, nodeName);
                                    return nodeName;
                                } else if ("PENDING".equals(state) || "CONFIGURING".equals(state)) {
                                    System.out.printf("[SlurmDriver] Job %s waiting (Pending)... try later \n", jobId);
                                }
                            }
                        }
                    }

                    // job disappeared
                    if (squeueStdout.isEmpty()) {
                        String sacctCmd = "sacct -j " + jobId + " -X -o \"State,NodeList\" -n -P";
                        try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                            net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(sacctCmd);
                            String sacctStdout = IOUtils.readFully(cmd.getInputStream()).toString().trim();
                            cmd.join(5, TimeUnit.SECONDS);

                            if (cmd.getExitStatus() == 0 && !sacctStdout.isEmpty()) {
                                String[] lines = sacctStdout.split("\\r?\\n");
                                if (lines.length > 0) {
                                    String[] parts = lines[0].split("\\|");
                                    String state = parts[0].toUpperCase();

                                    //
                                    if (state.startsWith("FAILED") || state.startsWith("CANCELLED") || state.startsWith("TIMEOUT") || state.startsWith("NODE_FAIL")) {
                                        throw new IOException("Slurm job in queue terminated: " + state);
                                    } else if (state.startsWith("COMPLETED")) {
                                        throw new IOException("Slurm terminated before java instruction");
                                    } else if (state.startsWith("RUNNING") && parts.length >= 2) {
                                        return parts[1].trim();
                                    }
                                }
                            }
                        }
                    }

                    Thread.sleep(3000);
                }
                throw new java.util.concurrent.TimeoutException("wait Job " + jobId + " over timeline " + timeoutSeconds + "s");
            } finally {
                if (ssh.isConnected()) {
                    ssh.disconnect();
                }
            }
        }




        public static void uploadScriptFile(String host, String username, String password,
                                            String localFilePath, String remoteFilePath) throws Exception {

            net.schmizz.sshj.SSHClient ssh = new net.schmizz.sshj.SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.setConnectTimeout(5000);

            try {
                System.out.printf("[SlurmDriver] connect to %s, prepare to file transfer...\n", host);
                ssh.connect(host); // 22
                ssh.authPassword(username, password);

                // TODO sftp
                try (SFTPClient sftp = ssh.newSFTPClient()) {
                    System.out.printf("[SlurmDriver] SFTP channel completed, upload: %s -> %s\n", localFilePath, remoteFilePath);

                    //
                    sftp.put(localFilePath, remoteFilePath);

                    System.out.println("[SlurmDriver] File Transfer done！");
                }

                // TODO chmod 755
                try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                    String chmodCmd = "chmod +x " + remoteFilePath;
                    System.out.println("[SlurmDriver] Chmod batch File: " + chmodCmd);

                    net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(chmodCmd);
                    cmd.join(5, TimeUnit.SECONDS);

                    if (cmd.getExitStatus() == 0) {
                        System.out.println("[SlurmDriver] chmod 755！");
                    } else {
                        System.err.println("[SlurmDriver] chmod failed " + cmd.getExitStatus());
                    }
                }

            } finally {
                if (ssh.isConnected()) {
                    ssh.disconnect();
                }
            }
        }


        public static boolean cancelJobStatic(String serverURL, String username, String password, String jobId) {
            if (jobId == null || jobId.trim().isEmpty()) {
                System.err.println("Job Id  invalid");
                return false;
            }

            net.schmizz.sshj.SSHClient ssh = new net.schmizz.sshj.SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());


            ssh.setConnectTimeout(5000);
            ssh.setTimeout(10000);

            String commandStr = "scancel " + jobId.trim();


            try {
                // handshake
                ssh.connect(serverURL);
                ssh.authPassword(username, password);

                try (net.schmizz.sshj.connection.channel.direct.Session session = ssh.startSession()) {
                    net.schmizz.sshj.connection.channel.direct.Session.Command cmd = session.exec(commandStr);
                    String stdout = IOUtils.readFully(cmd.getInputStream()).toString().trim();
                    String stderr = IOUtils.readFully(cmd.getErrorStream()).toString().trim();
                    cmd.join(5, TimeUnit.SECONDS);
                    int exitStatus = cmd.getExitStatus();

                    if (exitStatus == 0) {
                        System.out.printf("[SSH] cancel job Exit Code 0)。\n", jobId);
                        return true;
                    } else {
                        System.err.printf("[SSH] cancel job (Exit Code %d) \n", exitStatus, stderr);
                        return false;
                    }
                }
            } catch (Exception e) {
                System.err.printf("[SSH] cancel failed (Job: %s).  %s\n", jobId, e.getMessage());
                return false;
            } finally {

                if (ssh.isConnected()) {
                    try {
                        ssh.disconnect();
                    } catch (IOException ignored) {}
                }
            }
        }














       public static void main(String[] args) throws Exception {
            List<String> test = List.of("pwd", "sinfo", "pwd");

            List<String> batchCommands = Arrays.asList(
                    "sbatch <<'EOF'\n" +
                            "#!/bin/bash\n" +
                            "#SBATCH --job-name=DUUI_Batch_Test_1\n" +
                            "sleep 2\n" +
                            "EOF",

                    "sbatch <<'EOF'\n" +
                            "#!/bin/bash\n" +
                            "#SBATCH --job-name=DUUI_Batch_Test_2\n" +
                            "sleep 2\n" +
                            "EOF"
            );
            List<String> strings = submitMultipleJobs("localhost", "jd", "xxxxx", batchCommands);

           System.out.println(strings);
           waitForNodeAllocationSshj("localhost", "jd", "xxxxx","17",100);
           SSHHelper helper = new SSHHelper("|","|",22,"xxxxxx");

        }




    }


}

