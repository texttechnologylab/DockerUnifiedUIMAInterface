package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Request;
import okhttp3.Response;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster.SlurmSSHBatch;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

public class SlurmRestPhysical extends AbstractSlurm {
    private JobReader reader;
    protected String RESTVERSION_REMOTE;
    protected int RESTPORT_REMOTE;
    protected String PRE_URL_REMOTE;
    protected String URL_REMOTE;
    protected boolean REST_AVA_REMOTE;
    public SlurmRestPhysical(ConfigManager m, String password) {
        super(m, password);
        PhysicalMode config = m.getConfig().getPhysicalMode();
        this.RESTVERSION = config.getLocalCluster().getRestConfig().getVersion();
        this.RESTPORT = config.getLocalCluster().getRestConfig().getRestPort();
        this.PRE_URL = config.getLocalCluster().getPre_url();
        this.URL = PRE_URL + ":" + RESTPORT + "/slurm/" + RESTVERSION + "/";

        this.RESTVERSION_REMOTE = config.getRemoteCluster().getRestConfig().getVersion();
        this.RESTPORT_REMOTE = config.getRemoteCluster().getRestConfig().getRestPort();
        this.PRE_URL_REMOTE = config.getRemoteCluster().getPre_url();
        this.REST_AVA_REMOTE = config.getRemoteCluster().getRestConfig().isAvailable();
        this.URL_REMOTE = PRE_URL_REMOTE + ":" + RESTPORT_REMOTE + "/slurm/" + RESTVERSION_REMOTE + "/";
    }

    public SlurmRestPhysical(ConfigManager m, String password, JobReader reader) {
        super(m, password);
        PhysicalMode config = m.getConfig().getPhysicalMode();
        this.RESTVERSION = config.getLocalCluster().getRestConfig().getVersion();
        this.RESTPORT = config.getLocalCluster().getRestConfig().getRestPort();
        this.PRE_URL = config.getLocalCluster().getPre_url();
        this.URL = PRE_URL + ":" + RESTPORT + "/slurm/" + RESTVERSION + "/";
        this.reader = reader;

        this.RESTVERSION_REMOTE = config.getRemoteCluster().getRestConfig().getVersion();
        this.RESTPORT_REMOTE = config.getRemoteCluster().getRestConfig().getRestPort();
        this.PRE_URL_REMOTE = config.getRemoteCluster().getPre_url();
        this.URL_REMOTE = PRE_URL_REMOTE + ":" + RESTPORT_REMOTE + "/slurm/" + RESTVERSION_REMOTE + "/";
    }

    public JobReader getReader() {
        return reader;
    }

    public boolean checkRESTD_Remote() throws IOException {
        return this.REST_AVA_REMOTE;
    }

    /**
     * if the administrator has registered your user with the compute node, you can use your local name,
     *      * otherwise you can only use it as root
     * @return
     * @throws IOException
     */

    /**
     * if the administrator has registered your user with the compute node, you can use your local name, otherwise you can only use it as root
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */

    public String generateTokenByAdmin() throws IOException, InterruptedException {

        String hostName = showHostName();

        String arg = "username=".concat(hostName);
        String[] comms = new String[]{"scontrol", "token", arg};
        ProcessBuilder processBuilder = new ProcessBuilder(comms);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String result;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            result = reader.lines().collect(Collectors.joining("\n")).trim();
        }

        String[] split = result.split("=", 2);
        return split[1].trim();
    }


    public String generateTokenByHost() throws IOException, InterruptedException {

        String[] comms = new String[]{"scontrol", "token"};
        ProcessBuilder processBuilder = new ProcessBuilder(comms);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String result;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            result = reader.lines().collect(Collectors.joining("\n")).trim();
        }

        String[] split = result.split("=", 2);
        return split[1].trim();
    }

    public String generateTokenByHost_Remote() throws IOException, InterruptedException {

        String[] comms = new String[]{"scontrol", "token"};
        ProcessBuilder processBuilder = new ProcessBuilder(comms);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        String result;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            result = reader.lines().collect(Collectors.joining("\n")).trim();
        }

        String[] split = result.split("=", 2);
        return split[1].trim();
    }

    public boolean cancelJobSSH(String id) throws IOException {
        JobConfig jobConfig = this.getReader().loadConfig();
        String serverPath = jobConfig.getServer();
        String password = jobConfig.getPassword();
        String sshPort = jobConfig.getSshPort();
        String username = jobConfig.getUsername();
        boolean b = SlurmSSHBatch.SSHHelper.cancelJobStatic(serverPath, username, password, id);
        return b;


    }

    public boolean cancelJob_Remote(String jobID) throws IOException {
        if (checkRESTD_Remote()) {
            String cancelUrl = "job/".concat(jobID);
            Request req = new Request.Builder()
                    .url(URL.concat(cancelUrl))
                    .addHeader("X-SLURM-USER-TOKEN", this.getPassword_token())
                    .delete()
                    .build();
            try (Response response = httpClient.newCall(req).execute()) {
                if (!response.isSuccessful()) {
                    throw new IOException("HTTP " + response.code() + " - " + response.message()
                            + "\n" + response.body().string());
                }
                String respStr = response.body().string();
                //System.out.println(respStr);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(respStr);
                if (!jsonNode.hasNonNull("error")) {
                    System.out.println("[SlurmDriver] Cancel " + jobID + " Successfully");
                    return true;
                } else if (jsonNode.get("status").get("error").get("code").asInt() == 2021) {
                    System.out.println("[SlurmDriver] Cancel " + jobID + " Successfully");
                    return true;
                } else {
                    System.out.println("[SlurmDriver] Failed to Cancel " + jobID + " , see logs");
                    return false;
                }

            }
        }
        return false;
    }

    public String getRESTVERSION_REMOTE() {
        return RESTVERSION_REMOTE;
    }

    public int getRESTPORT_REMOTE() {
        return RESTPORT_REMOTE;
    }

    public String getPRE_URL_REMOTE() {
        return PRE_URL_REMOTE;
    }

    public String getURL_REMOTE() {
        return URL_REMOTE;
    }

    public boolean isREST_AVA_REMOTE() {
        return REST_AVA_REMOTE;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ConfigManager cm = ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml"));
        SlurmRestPhysical p = new SlurmRestPhysical(cm, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM5MzE2MjcxOTksImlhdCI6MTc4NDE0MzU1Mywic3VuIjoiamQifQ.iOH2wdmZB8ssbbFnHpIf-lYUtZ8gO4nfA5exlm3kbNM");
        System.out.println(p.showHostName());
        System.out.println(p.getRESTPORT());
        System.out.println(p.generateTokenByHost());
        System.out.println(p.generateTokenByAdmin());
        System.out.println(p.checkRESTD());
    }
}
