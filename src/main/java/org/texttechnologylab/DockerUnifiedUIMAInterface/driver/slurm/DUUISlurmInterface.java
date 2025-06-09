package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;

// funtionen: 1 use docker cli pull image falls nicht vorhanden
//            2 in slurm hat jede Aufgabe eindeutig ein jobid, man kann anhand id ein job starten oder stoppen，
//            also eine Verwalttung von Port und id sehr wichtig Hashmap
public class DUUISlurmInterface {

    private DockerClient _docker;

    private HashMap<String, String> _jobID_PortMap = new HashMap<>();

    public DUUISlurmInterface() throws IOException {
//
        if (!System.getProperty("os.name").contains("Windows")) {
            _docker = DockerClientBuilder.getInstance().build();
        } else {
            // Windows
            DockerHttpClient http = null;
            try {
                http = new ApacheDockerHttpClient.Builder()
                        .connectionTimeout(Duration.ofSeconds(5))
                        .responseTimeout(Duration.ofMinutes(10))
                        .dockerHost(URI.create("npipe:////./pipe/docker_engine"))
                        .build();
            } catch (Exception e) {
                http = new ApacheDockerHttpClient.Builder()
                        .connectionTimeout(Duration.ofSeconds(5))
                        .responseTimeout(Duration.ofMinutes(10))
                        .dockerHost(URI.create("tcp://127.0.0.1:2375")) // if npipe doesn't work.
                        .build();
            }
            _docker = DockerClientBuilder.getInstance()
                    .withDockerHttpClient(http)
                    .build();
        }


    }


    public boolean checkDependencies() {
        return SlurmUtils.checkSlurmInstalled() && SlurmUtils.checkSocatInstalled();
    }

   // Die Funktion run erstellt ein Shell-Skript, das dem Slurm-Jobsystem übergeben und dann ausgeführt wird.“
    public String run(DUUIPipelineComponent comp, int hostPort) throws IOException, InterruptedException {
        String gpu = comp.getSlurmGPU();
        String sifName = comp.getSlurmSIFImageName();
        String slurmJobName = comp.getSlurmJobName();
        String slurmEntryLocation = comp.getSlurmEntryLocation();
        String slurmErrorLocation = comp.getSlurmErrorLocation();
        String slurmImagePort = comp.getSlurmImagePort();
        String slurmCpus = comp.getSlurmCpus();
        String slurmRuntime = comp.getSlurmRuntime();
        String slurmMem = comp.getSlurmMem();
        String slurmSaveIn = comp.getSlurmSIFDiskLocation();
        String slurmOutPutLocation = comp.getSlurmOutPutLocation();
        String slurmUvicorn = comp.getSlurmUvicorn();
//Diese Funktion konvertiert einen String in ein Shell Format.
        String jobid = SlurmUtils.submitJob(
                slurmJobName,
                Integer.toString(hostPort),
                slurmCpus, gpu, slurmEntryLocation,
                slurmErrorLocation,
                slurmImagePort,
                slurmMem,
                slurmRuntime,
                slurmOutPutLocation,
                sifName,
                slurmSaveIn,
                slurmUvicorn
        );

        System.out.println("job submitted ready for release, jobid : " + jobid);
       // neue Abbildung gespeichert
        _jobID_PortMap.put(jobid, Integer.toString(hostPort));

        return jobid;
    }


    public String extractPort(String jobid){
        return  _jobID_PortMap.get(jobid);
    }

    public DockerClient get_docker() {
        return _docker;
    }

    public HashMap<String, String> get_jobID_PortMap() {
        return _jobID_PortMap;
    }

}

