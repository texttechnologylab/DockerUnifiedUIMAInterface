package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;

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
import java.util.List;
import java.util.Map;

public class SlurmUtils {
// ein paar cli command in java umschreiben/kaspseln
    public static boolean isMasterNode(){
        String s = whoIsMaster().split("=", 2)[1];
        return s.equals(whoAmI());}

    public static String  whoIsMaster(){
        try {
            InputStream pb = new ProcessBuilder("bash", "-c", "cat /etc/slurm/slurm.conf | grep ControlMachine").start().getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pb));
            return reader.readLine();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String whoAmI(){
        try {
            InputStream pb = new ProcessBuilder("hostname").start().getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pb));
            return reader.readLine();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean pullSifImagefromRemoteDockerRepo(String imagePath, String imageName) {
        if (imagePath == null || imagePath.isEmpty()) {
            throw new IllegalArgumentException("imagePath is null or empty");
        }

        System.out.println("Correct format: apptainer build {Sif_image_name}.sif docker://{docker_repo/image:tag}");

        List<String> command = List.of(
                "apptainer", "build",
                imageName + ".sif",
                "docker://" + imagePath
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


    public static boolean pullSifImagefromLocalDockerRepo(String imagePath, String imageName) {
        if (imagePath == null || imagePath.isEmpty()) {
            throw new IllegalArgumentException("imagePath is null or empty");
        }

        System.out.println("Correct format: apptainer build {Sif_image_name}.sif docker-daemon://{image:tag}");

        List<String> command = List.of(
                "apptainer", "build",
                imageName + ".sif",
                "docker-daemon://" + imagePath
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


    public static boolean submitHeldJob(String scriptPath) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("sbatch", "--hold", scriptPath);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Submitted batch job")) {
                    return true;
                }
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("sbatch submission failed");
        }
        return false;
    }


//    public static void releaseJob(String jobId) throws IOException, InterruptedException {
//        ProcessBuilder pb = new ProcessBuilder("scontrol", "release", jobId);
//        pb.environment().put("SLURM_JOB_ID", jobId);
//        pb.inheritIO();
//        Process process = pb.start();
//        int exitCode = process.waitFor();
//        if (exitCode != 0) {
//            throw new RuntimeException("Failed to release job " + jobId);
//        }
//    }


    public static List<Integer> getHeldJobs() throws IOException, InterruptedException {
        List<Integer> jobIds = new ArrayList<>();
        ProcessBuilder pb = new ProcessBuilder("squeue", "--user=" + System.getProperty("user.name"), "--state=PD", "-h", "-o", "%i");
        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                jobIds.add(Integer.parseInt(line.trim()));
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Failed to fetch held jobs");
        }

        return jobIds;
    }
//
//    public static void releaseAllHeldJobs() throws IOException, InterruptedException {
//        List<Integer> heldJobs = getHeldJobs();
//        for (int jobId : heldJobs) {
//            releaseJob(jobId);
//        }
//    }
//



    public static void squeue() throws IOException {
        ProcessBuilder pb = new ProcessBuilder("squeue");
        pb.inheritIO();
        pb.start();
    }


    public static void cancelJob(int jobId) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("scancel", String.valueOf(jobId));
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Failed to cancel job " + jobId);
        }
    }




   // Ich finde das ziemlich zeitaufwendig, deshalb möchte ich später eine ähnliche Webseite für duui entwickeln
    //Zum Beispiel könnte man den Namen einer DUUI-Komponente eingeben, und das System würde automatisch eine passende
  //  Konfiguration mit Entrypoint vorschlagen oder vervollständigen.
    public static String submitJob(String jobName,String hostport,
                                   String cpus,String gpu,String entry, String error, String imageport,String mem,
                                   String time,
                                   String output,String sifname, String saveIn, String uvicorn
                                   ) throws IOException, InterruptedException {

        Path script = Paths.get("/tmp", jobName + ".sh");


        List<String> lines = List.of(
                "#!/bin/bash",
                "#SBATCH --job-name=" + jobName,
                "#SBATCH --cpus-per-task=" + cpus,
                "#SBATCH --time=" + time,
                "",
                "PORT=" + hostport,
                "UVI=\"" + uvicorn + "\"",               //
                "INNER=" + imageport,
                "IMG=\"" + saveIn + "\"",
                "INTOIMAGE=\"" + entry + "\"",           // looks like "cd /usr/src/app"
                "",
                "apptainer exec \"$IMG\" \\",
                "  sh -c \"$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER\" &",
                "PID=$!",
                "",
                "socat TCP-LISTEN:$PORT,reuseaddr,fork TCP:127.0.0.1:$INNER &",
                "PID_SOCAT=$!",
                "",
                "trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT",
                "",
                "wait $PID"
        );


      Files.write(script, lines, StandardCharsets.UTF_8,
              StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
      script.toFile().setExecutable(true);
      System.out.println("Slurm batch script written to: " + script);


      ProcessBuilder pb = new ProcessBuilder("sbatch", "--parsable",script.toString());
      pb.redirectErrorStream(true);
      Process proc = pb.start();

      String jobId;
      try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
         jobId = br.readLine();
      }
      int exit = proc.waitFor();
      if (exit != 0 || jobId == null || jobId.isBlank()) {
         throw new IllegalStateException("sbatch failed，exit=" + exit);
      }

      System.out.println("Job submitted. ID = " + jobId);
      return jobId.trim();
   }

    public static boolean checkSocatInstalled(){
        try {
            InputStream pb = new ProcessBuilder("which", "socat").start().getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pb));
            String s = reader.readLine();
            System.out.println(s);
            return !s.isEmpty();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static boolean checkSlurmInstalled(){
        try {
            InputStream pb = new ProcessBuilder("slurmd", " ", "-C").start().getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pb));
            String s = reader.readLine();
            System.out.println(s);
            return !s.isEmpty();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }







}
