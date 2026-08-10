package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.RestConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSlurm implements SlurmRest {
    private String password_token;
    protected ConfigManager manager;
    protected  String RESTVERSION;
    protected  int RESTPORT;
    protected  String PRE_URL;
    protected  String URL;
    protected ServiceDetector detector;
    protected static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .build();

    public AbstractSlurm(ConfigManager m, String password_token) {
        this.manager = m;
        this.detector = new ServiceDetector();
        RestConfig config = m.getConfig().getContainerMode().getRestConfig();
        this.PRE_URL = m.getConfig().getContainerMode().getPreURL();
        this.RESTVERSION = config.getVersion();
        this.RESTPORT = config.getRestPort();
        this.URL = PRE_URL + ":" + RESTPORT + "/slurm/" + RESTVERSION + "/";
        this.password_token = password_token;
    }


    // ==================================>inner class begin, special for containerize slurm
    public class ServiceDetector {
        public OkHttpClient httpClient = AbstractSlurm.httpClient;

        public boolean detect() throws IOException {
            Request request = new Request.Builder()
                    .url(PRE_URL + ":" + RESTPORT)
                    .head()
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                // if we can read header then analyze status code
                return response.code() < 500;
            } catch (java.io.IOException e) {
                String msg = e.getMessage();
                // unexpected end of stream  Connection reset, service alive but no permission
                if (msg != null && (msg.contains("unexpected end of stream") || msg.contains("Connection reset"))) {
                    System.out.println("ONLINE, but no password");
                    return true;
                }
            }
            return false;
        }

    }


    // inner class over<======================================
    @Override
    public String showHostName() throws IOException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("whoami");
        Process start = pb.redirectErrorStream(true).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(start.getInputStream()));
        return br.readLine();
    }


    @Override
    public String query(String where) {
        Request req = new Request.Builder().url(URL.concat(where)).header("X-SLURM-USER-TOKEN", password_token).get().build();
        try (Response response = httpClient.newCall(req).execute()) {
            return response.body().string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean cancelJob(String jobID) throws IOException {
        String cancelUrl = "job/".concat(jobID);
        Request req = new Request.Builder()
                .url(URL.concat(cancelUrl))
                .addHeader("X-SLURM-USER-TOKEN", password_token)
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

    @Override
    public String[] submit(JSONObject params) throws IOException {
        MediaType type = MediaType.get("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(null, params.toString());

        Request req = new Request.Builder().url(URL.concat("job/submit")).
                addHeader("X-SLURM-USER-TOKEN", password_token).
                addHeader("Content-Type", "application/json").
                post(requestBody).
                build();
        try (Response response = httpClient.newCall(req).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("HTTP " + response.code() + " - " + response.message()
                        + "\n" + response.body().string());
            }

            String respStr = response.body().string();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(respStr);
            String jobId = jsonNode.get("job_id").asText();
            return new String[]{respStr, jobId};

        }
    }

    public boolean checkRESTD() throws IOException {
        return this.detector.detect();
    }

    public String getPassword_token() {
        return password_token;
    }

    public ConfigManager getManager() {
        return manager;
    }

    public String getRESTVERSION() {
        return RESTVERSION;
    }

    public int getRESTPORT() {
        return RESTPORT;
    }

    public String getPRE_URL() {
        return PRE_URL;
    }

    public String getURL() {
        return URL;
    }

    public ServiceDetector getDetector() {
        return detector;
    }
}
