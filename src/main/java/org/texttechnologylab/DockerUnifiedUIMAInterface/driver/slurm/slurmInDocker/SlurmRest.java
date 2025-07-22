package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.slurmInDocker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DockerClientBuilder;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;
import org.javatuples.Tuple;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SlurmRest {
    private DockerClient dockerClient = DockerClientBuilder.getInstance().build();
    private final String RESTVERSION = "v0.0.42";
    private final String PRE_URL;
    private final String URL;
    OkHttpClient httpClient = new OkHttpClient();

    /**
     *
     * @param preUrl if slurmrestd on host preurl is http:localhost
     */
    public SlurmRest(String preUrl) {
        PRE_URL = preUrl;
        URL = PRE_URL + ":6820/slurm/" + RESTVERSION + "/";
    }

    /**
     *
     * @return container name -- docker container id
     */
    public Map<String, String> containerNameID() {
        Map<String, String> nameID = new HashMap<>();
        List<Container> exec = dockerClient.listContainersCmd().exec();
        exec.stream().forEach((c) -> {
            String[] names = c.getNames();// [/xxx]
            //System.out.println(names[0]);
            Pattern p = Pattern.compile("^/([a-zA-Z0-9]*)$");
            Matcher m = p.matcher(names[0]);
            String result = m.find() ? m.group(1) : "";
            String id = c.getId();
            nameID.put(result, id);
        });
        return nameID;
    }

    /**
     *
     * @return all containers names
     */
    public List<String> listContainerNames() {
        List<Container> exec = dockerClient.listContainersCmd().exec();
        List<String> containers = new ArrayList<>();
        exec.stream().forEach(container -> {
            containers.add(Arrays.toString(container.getNames()));
        });
        return containers;
    }

    /**
     *
      * @return check restd as container running in cluster
     */
    public boolean checkRESTD() {
        List<String> containers = listContainerNames();
        return containers.stream().anyMatch(containerName -> containerName.contains("rest"));
    }

    /**
     * if the administrator has registered your user with the compute node, you can use your local name,
     *      * otherwise you can only use it as root
     * @return
     * @throws IOException
     */
    public String showHostName() throws IOException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command("whoami");
        Process start = pb.redirectErrorStream(true).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(start.getInputStream()));
        return br.readLine();
    }

    /**
     * if the administrator has registered your user with the compute node, you can use your local name, otherwise you can only use it as root
     * @param containerName Nodes that can generate jwt tokens for users include compute and slurmctld
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String generateTokenByHost(String containerName) throws IOException, InterruptedException {
        String hostName = showHostName();
        String arg = "username=".concat(hostName);
        String[] comms = new String[]{"scontrol", "token", arg};
        // default life-time is 5min
        String token = executeInContainer(containerName, comms);
        String[] split = token.split("=", 2);
        return split[1].trim();
    }

    /**
     * if the administrator has registered your user with the compute node, you can use your local name, otherwise you can only use it as root
     * @param containerName Nodes that can generate jwt tokens for users include compute and slurmctld
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String generateRootToken(String containerName) throws IOException, InterruptedException {
        String arg = "username=".concat("root");
        String[] comms = new String[]{"scontrol", "token", arg};
        String token = executeInContainer(containerName, comms);
        String[] split = token.split("=", 2);
        return split[1].trim();
    }

    public String executeInContainer(String containerName, String[] commands) throws IOException, InterruptedException {
        Map<String, String> nameIDMap = containerNameID();
        String containerId = nameIDMap.get(containerName);
        ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withCmd(commands)
                .exec();

        StringBuilder result = new StringBuilder();
        dockerClient.execStartCmd(execCreateCmdResponse.getId())
                .exec(new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame frame) {
                        result.append(new String(frame.getPayload()));
                    }
                }).awaitCompletion();
        return result.toString();
    }

    /**
     *
     * @param token jwt password
     * @param where Specify permitted format queries here, https://slurm.schedmd.com/rest.html
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String query(String token, String where) throws IOException, InterruptedException {
        Request req = new Request.Builder().url(URL.concat(where)).header("X-SLURM-USER-TOKEN", token).get().build();
        try (Response response = httpClient.newCall(req).execute()) {
            return response.body().string();
        }
    }

    /**
     * Specify permitted format queries here, https://slurm.schedmd.com/rest.html
     * @param token
     * @param params
     * @return [0]:full json response,  [1]: job id
     * @throws IOException
     * @throws InterruptedException
     */
    public String[] submit(String token, JSONObject params) throws IOException, InterruptedException {
        MediaType type = MediaType.get("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(null, params.toString());

        Request req = new Request.Builder().url(URL.concat("job/submit")).
                addHeader("X-SLURM-USER-TOKEN", token).
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

    /**
     * cancel x-th job
     * @param token
     * @param jobID
     * @return
     * @throws IOException
     */
    public boolean cancelJob(String token, String jobID) throws IOException {
        String cancelUrl = "job/".concat(jobID);
        Request req = new Request.Builder()
                .url(URL.concat(cancelUrl))
                .addHeader("X-SLURM-USER-TOKEN", token)
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
            if (!jsonNode.hasNonNull("error")){
                System.out.println("[SlurmDriver] Cancel "+jobID+" Successfully");
                return true;
            }
            else if(jsonNode.get("status").get("error").get("code").asInt()==2021){
                System.out.println("[SlurmDriver] Cancel "+jobID+" Successfully");
                return true;
            }
            else {
                System.out.println("[SlurmDriver] Failed to Cancel "+jobID+" , see logs");
                return false;
            }
        }
    }
}


