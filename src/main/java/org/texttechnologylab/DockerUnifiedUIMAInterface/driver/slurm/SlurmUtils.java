package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import io.swagger.util.Json;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.slurmInDocker.SlurmRest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SlurmUtils {
    public static boolean isDockerImagePresent(String imageName) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("docker", "image", "inspect", imageName);
        Process start = pb.start();
        return start.waitFor() == 0;

    }

    public static boolean pullSifImagefromRemoteDockerRepo(String dockerImageName, String sifImagePath) {
        if (sifImagePath == null || sifImagePath.isEmpty()) {
            throw new IllegalArgumentException("imagePath is null or empty");
        }

        System.out.println("Correct format: apptainer build {Sif_image_name}.sif docker://{docker_repo/image:tag}");

        List<String> command = List.of(
                "apptainer", "build",
                sifImagePath,
                "docker://" + dockerImageName
        );
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        pb.inheritIO();
        try {
            Process process = pb.start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to run apptainer build", e);
        }
    }


    public static boolean pullSifImagefromLocalDockerRepo(String dockerImageName, String sifImagePath) {
        if (dockerImageName == null || dockerImageName.isEmpty()) {
            throw new IllegalArgumentException("dockerImageName is null or empty");
        }

        System.out.println("Correct format: apptainer build {Sif_image_name}.sif docker-daemon://{image:tag}");

        List<String> command = List.of(
                "apptainer", "build",
                sifImagePath,
                "docker-daemon://" + dockerImageName
        );

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        pb.inheritIO();
        try {
            Process process = pb.start();
            int exitCode = process.waitFor();
            return exitCode == 0;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to run apptainer build", e);
        }
    }



    public static JSONObject generateJobScript_GPU(
            String jobname,
            String runtime,
            String workdir,
            String hostPort,
            String sif_location,
            String uvicornCmd,
            String memory,
            String gpu,
            String sifimagename,
            String partitionname,
            String whichnode,
            String cpus
    ) {
        int minutes = Integer.parseInt(runtime);
        int mems = Integer.parseInt(memory);

        int outer = Integer.parseInt(hostPort);
        JSONObject job = new JSONObject()
                .put("name", jobname)
                .put("partition", partitionname)
                .put("time_limit", minutes)
                .put("current_working_directory", workdir)
                .put("cpus_per_task", Integer.parseInt(cpus))
                .put("nodes", "1")
                .put("required_nodes", List.of(whichnode))
                .put("memory_per_node", new JSONObject()
                        .put("set", true)
                        .put("number", mems))
                .put( "tres_per_node", "gres/gpu="+gpu)
                .put("standard_output", workdir+"/"+sifimagename+"/"+sifimagename+".out")
                .put("standard_error", workdir+"/"+sifimagename+"/"+sifimagename+".err")
                .put("environment",
                        new JSONArray().put("PATH=/bin:/usr/bin:/sbin:$PATH"));

        String script = """
                #!/bin/bash -e
                find_free() { for p in $(seq 30000 31000); do ss -ltn "( sport = :$p )" | grep -q LISTEN || { echo $p; return; }; done; }
                INNER=$(find_free) 
                PORT=%d
                IMG="%s"
                INTOIMAGE="cd /usr/src/app"
                UVI="%s"
                
                apptainer exec "$IMG" \\
                sh -c "$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER" &
                PID=$!
                
                socat TCP-LISTEN:$PORT,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:$INNER &
                PID_SOCAT=$!
                
                trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT
                wait $PID
                """.formatted(outer, sif_location, uvicornCmd);

        return new JSONObject()
                .put("script", script)
                .put("job", job);
    }



    public static JSONObject generateJobScript_non_GPU(
            String jobname,
            String runtime,
            String workdir,
            String hostPort,
            String sif_location,
            String uvicornCmd,
            String memory,
            // String gpu,
            String sifimagename,
            String partitionname,
            String whichnode,
            String cpus
    ) {
        int minutes = Integer.parseInt(runtime);
        int mems = Integer.parseInt(memory);
        int outer = Integer.parseInt(hostPort);
        JSONObject job = new JSONObject()
                .put("name", jobname)
                .put("partition", partitionname)
                .put("time_limit", minutes)
                .put("cpus_per_task", Integer.parseInt(cpus))
                .put("current_working_directory", workdir)
                .put("nodes", "1")
                .put("required_nodes", List.of(whichnode))
                .put("memory_per_node", new JSONObject()
                        .put("set", true)
                        .put("number", mems))
                .put("standard_output", workdir+"/"+sifimagename+"/"+sifimagename+".out")
                .put("standard_error", workdir+"/"+sifimagename+"/"+sifimagename+".err")
                .put("environment",
                        new JSONArray().put("PATH=/bin:/usr/bin:/sbin:$PATH"));

        String script = """
                #!/bin/bash -e
                find_free() { for p in $(seq 30000 31000); do ss -ltn "( sport = :$p )" | grep -q LISTEN || { echo $p; return; }; done; }
                INNER=$(find_free) 
                PORT=%d  
                IMG="%s"
                INTOIMAGE="cd /usr/src/app"
                UVI="%s"
                
                apptainer exec "$IMG" \\
                sh -c "$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER" &
                PID=$!
                
                socat TCP-LISTEN:$PORT,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:$INNER &
                PID_SOCAT=$!
                
                trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT
                wait $PID
                """.formatted(outer, sif_location, uvicornCmd);

        // ---------- 3. payload ----------
        return new JSONObject()
                .put("script", script)
                .put("job", job);
    }


}
