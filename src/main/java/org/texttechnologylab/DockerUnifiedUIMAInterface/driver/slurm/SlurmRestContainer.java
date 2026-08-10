package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DockerClientBuilder;
import okhttp3.*;

import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SlurmRestContainer extends AbstractSlurm{

    private DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    public SlurmRestContainer(ConfigManager m, String pass) {
        super(m, pass);
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



    /**
     * if the administrator has registered your user with the compute node, you can use your local name, otherwise you can only use it as root
     *
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
     *
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

    }



