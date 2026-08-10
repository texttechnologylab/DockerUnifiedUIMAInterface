package slurmDriver;

import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster.SSHForwarder;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster.SlurmSSHBatch;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmRest;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmRestPhysical;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.JobReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.SlurmConfig;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SbatchSSHTest {
    ConfigManager manager  = ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml"));

    JobReader jobReader = new JobReader("src/main/resources/slurmDriverConf/jobs.yaml");

    SlurmRestPhysical sphi = new SlurmRestPhysical(manager, "", jobReader);
    SlurmRestPhysical sphi_rest = new SlurmRestPhysical(manager, "password", jobReader);

    public SbatchSSHTest() throws IOException {
    }


    /*
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
false
http://localhost
[main] INFO net.schmizz.sshj.transport.random.JCERandom - Creating new SecureRandom.
[SlurmDriver] Connect Cluster localhost...
[main] INFO net.schmizz.sshj.transport.TransportImpl - Client identity string: SSH-2.0-SSHJ_0.38.0
[main] INFO net.schmizz.sshj.transport.TransportImpl - Server identity string: SSH-2.0-OpenSSH_9.9p1 Ubuntu-3ubuntu3.2
[SlurmDriver] Submit 2 Job...
[SlurmDriver] submit 27
[SlurmDriver] submit 28
[main] INFO net.schmizz.sshj.transport.TransportImpl - Disconnected - BY_APPLICATION
[27, 28]

     */
    @Test
    public void SbatchSSHTest() throws Exception {
        System.out.println(sphi.checkRESTD_Remote());
        System.out.println(sphi.getPRE_URL_REMOTE());
        String servername = sphi.getReader().loadConfig().getServer();
        String username = sphi.getReader().loadConfig().getUsername();
        String password = sphi.getReader().loadConfig().getPassword();
        String sshPort = sphi.getReader().loadConfig().getSshPort();
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

        List<String> strings = SlurmSSHBatch.SSHHelper.submitMultipleJobs(servername, username, password, batchCommands);
        System.out.println(strings);


    }
    @Test
    public void checkNodes() throws Exception {
        System.out.println(sphi.checkRESTD_Remote());
        System.out.println(sphi.getPRE_URL_REMOTE());
        String servername = sphi.getReader().loadConfig().getServer();
        String username = sphi.getReader().loadConfig().getUsername();
        String password = sphi.getReader().loadConfig().getPassword();
        String sshPort = sphi.getReader().loadConfig().getSshPort();
        List<String> batchCommands = Arrays.asList(
                "sbatch <<'EOF'\n" +
                        "#!/bin/bash\n" +
                        "#SBATCH --job-name=DUUI_Batch_Test_1\n" +
                        "#SBATCH --time=00:00:15\n" +
                        "sleep 15\n" +
                        "EOF"
        );
        List<String> ids = SlurmSSHBatch.SSHHelper.submitMultipleJobs(servername, username, password, batchCommands);
       String nodeName = SlurmSSHBatch.SSHHelper.blockUntilRunning(servername,
               username, password, ids.get(0), 10, new AtomicBoolean(false));

        System.out.println(nodeName);
/*
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
false
http://localhost
[main] INFO net.schmizz.sshj.transport.random.JCERandom - Creating new SecureRandom.
[SlurmDriver] Connect Cluster localhost...
[main] INFO net.schmizz.sshj.transport.TransportImpl - Client identity string: SSH-2.0-SSHJ_0.38.0
[main] INFO net.schmizz.sshj.transport.TransportImpl - Server identity string: SSH-2.0-OpenSSH_9.9p1 Ubuntu-3ubuntu3.2
[SlurmDriver] Submit 1 Job...
[SlurmDriver] submit 29
[main] INFO net.schmizz.sshj.transport.TransportImpl - Disconnected - BY_APPLICATION
[main] INFO net.schmizz.sshj.transport.random.JCERandom - Creating new SecureRandom.
[SlurmDriver]wait until resource allocated
[main] INFO net.schmizz.sshj.transport.TransportImpl - Client identity string: SSH-2.0-SSHJ_0.38.0
[main] INFO net.schmizz.sshj.transport.TransportImpl - Server identity string: SSH-2.0-OpenSSH_9.9p1 Ubuntu-3ubuntu3.2
[SlurmDriver] Resource allocated！Job 29 on [jd-Inspiron-14-5410]
[main] INFO net.schmizz.sshj.transport.TransportImpl - Disconnected - BY_APPLICATION
jd-Inspiron-14-5410
 */
    }

    @Test
    public void SSHForwarder() throws Exception {
        SSHForwarder forwarder = new SSHForwarder();
        String servername = sphi.getReader().loadConfig().getServer();
        String username = sphi.getReader().loadConfig().getUsername();
        String password = sphi.getReader().loadConfig().getPassword();
        String sshPort = sphi.getReader().loadConfig().getSshPort();

        forwarder.start(9090, password,username,"jd-Inspiron-14-5410", 8080, "localhost"  );
/*
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
[main] INFO net.schmizz.sshj.transport.random.JCERandom - Creating new SecureRandom.
[main] INFO net.schmizz.sshj.transport.TransportImpl - Client identity string: SSH-2.0-SSHJ_0.38.0
[main] INFO net.schmizz.sshj.transport.TransportImpl - Server identity string: SSH-2.0-OpenSSH_9.9p1 Ubuntu-3ubuntu3.2
[DUUI-Tunnel-Daemon-9090] INFO net.schmizz.sshj.connection.channel.direct.LocalPortForwarder - Listening on /127.0.0.1:9090
[SSH-Tunnel] Daemeon for Forwarding：localhost:9090 ===> jd-Inspiron-14-5410:8080

 */
    }

    @Test
    public void transferText() throws Exception {
        String servername = sphi.getReader().loadConfig().getServer();
        String username = sphi.getReader().loadConfig().getUsername();
        String password = sphi.getReader().loadConfig().getPassword();
        String sshPort = sphi.getReader().loadConfig().getSshPort();
        String workDir = sphi.getReader().loadConfig().getWorkDir();
        String job = sphi.getReader().loadConfig().getJobs().get(0).replace("PLACEHOLDER", Integer.toString(20001));

        String serverWorkdir  = sphi.getReader().loadConfig().getWorkDir();
        String filename = "src/main/resources/slurmDriverConf/"+Integer.toString(20001)+".sh";
        SlurmUtils.saveStrAsFile(job, filename);
        SlurmSSHBatch.SSHHelper.uploadScriptFile(servername,username, password, filename,serverWorkdir );
        /*
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0
[main] INFO net.schmizz.sshj.transport.random.JCERandom - Creating new SecureRandom.
[SlurmDriver] connect to localhost, prepare to file transfer...
[main] INFO net.schmizz.sshj.transport.TransportImpl - Client identity string: SSH-2.0-SSHJ_0.38.0
[main] INFO net.schmizz.sshj.transport.TransportImpl - Server identity string: SSH-2.0-OpenSSH_9.9p1 Ubuntu-3ubuntu3.2
[main] INFO net.schmizz.sshj.connection.channel.direct.SessionChannel - Will request `sftp` subsystem
[SlurmDriver] SFTP channel completed, upload: src/main/resources/slurmDriverConf/20001.sh -> /home/jd
[SlurmDriver] File Transfer done！
[SlurmDriver] Chmod batch File: chmod +x /home/jd
[SlurmDriver] chmod 755！
[main] INFO net.schmizz.sshj.transport.TransportImpl - Disconnected - BY_APPLICATION

         */
    }
}
