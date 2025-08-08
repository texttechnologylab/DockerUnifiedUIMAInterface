package org.texttechnologylab.DockerUnifiedUIMAInterface.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DockerClientBuilder;
import org.apache.commons.lang.math.IntRange;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test helper class to create and run a container image for testing purposes.
 * Will search for a random free port
 */
public class DockerTestContainerManager implements AutoCloseable {
    final DockerClient dockerClient = DockerClientBuilder.getInstance().build();
    final CreateContainerResponse container;
    final int port = searchFreePort();

    public DockerTestContainerManager(String imageName) {
        this(imageName, "test-duui-" + UUID.randomUUID(), 5000);
    }

    public DockerTestContainerManager(String imageName, String containerName) {
        this(imageName, containerName, 5000);
    }

    public DockerTestContainerManager(String imageName, long startupDelay) {
        this(imageName, "test-duui-" + UUID.randomUUID(), startupDelay);
    }

    public DockerTestContainerManager(String imageName, String containerName, long startupDelay) {
        System.out.printf("[DockerTestContainerManager] Creating Container Image for %s%n", imageName);
        container = dockerClient.createContainerCmd(imageName)
                .withHostConfig(
                        HostConfig.newHostConfig()
                                .withAutoRemove(true)
                                .withPublishAllPorts(true)
                                .withPortBindings(PortBinding.parse("%d:9714".formatted(port)))
                )
                .withExposedPorts(ExposedPort.tcp(9714))
                .withName(containerName)
                .exec();
        System.out.printf("[DockerTestContainerManager] Container Image Created: %s%n", container.getId());
        System.out.printf("[DockerTestContainerManager] Starting Container as %s with port binding %d:9714%n", containerName, port);
        dockerClient.startContainerCmd(container.getId()).exec();

        try {
            System.out.printf("[DockerTestContainerManager] Waiting %dms for Container to Come Online%n", startupDelay);
            Thread.sleep(startupDelay);
        } catch (InterruptedException e) {
        }

        System.out.println("[DockerTestContainerManager] Container Started: " + container.getId());
    }

    private static int searchFreePort() {
        List<Integer> ports = Arrays.stream(new IntRange(30000, 40000).toArray()).boxed().collect(Collectors.toList());
        Collections.shuffle(ports);
        for (int port : ports) {
            try (ServerSocket socket = new ServerSocket(port)) {
                if (socket.getLocalPort() == port) {
                    return port;
                }
            } catch (IOException e) {
                if (!e.getMessage().contains("Address already in use")) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("No free port found");
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() throws Exception {
        System.out.println("[DockerTestContainerManager] Stopping Container: " + container.getId());
        dockerClient.stopContainerCmd(container.getId()).withTimeout(10).exec();
        System.out.println("[DockerTestContainerManager] Container Stopped");
        System.out.println("[DockerTestContainerManager] Stopping Docker Client");
        dockerClient.close();
    }

}