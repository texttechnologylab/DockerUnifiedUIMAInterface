import org.junit.jupiter.api.Test;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestKubernetes {

    private static String KUBECONFIGPATH = "/etc/kubernetes/admin.conf";

    public static void getKubeInfos(String sProcessName, File oPath) throws IOException {

        List<ProcessBuilder> processBuilders = new ArrayList<>(0);

        StringBuilder sb = new StringBuilder();

//        processBuilders.add(new ProcessBuilder("export", "KUBECONFIG=/etc/kubernetes/admin.conf"));
        processBuilders.add(new ProcessBuilder("kubectl", "get", "pods", "-o", "wide"));
        processBuilders.add(new ProcessBuilder("kubectl", "get", "deployments", "-o", "wide"));

        for (ProcessBuilder processBuilder : processBuilders) {
            Process p = null;
            try {

                Map<String, String> env = processBuilder.environment();
                env.put("KUBECONFIG", KUBECONFIGPATH);

                p = processBuilder.start();

                try {
                    // Create a new reader from the InputStream
                    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader br2 = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                    // Take in the input
                    String input;
                    while ((input = br.readLine()) != null) {
                        // Print the input
                        if (sb.length() > 0) {
                            sb.append("\n");
                        }
                        sb.append(input);
                    }
                    while ((input = br2.readLine()) != null) {
                        // Print the input
                        System.err.println(input);
                    }
                } catch (IOException io) {
                    io.printStackTrace();
                }

                p.waitFor();
            } catch (IOException | InterruptedException ex) {
                ex.printStackTrace();
            }

        }

        if (sb.length() > 0) {
            FileUtils.writeContent(sb.toString(), new File(oPath.getPath() + "/" + sProcessName + ".log"));
        }

        //docker run --gpus all nvidia/cuda:11.0-base nvidia-smi
    }

    @Test
    public void test() throws IOException {


    }


}
