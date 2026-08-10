package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SSHCluster;

import org.javatuples.Tuple;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmInterface;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmRestPhysical;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.JobConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class DUUISlurmClusterInterface {
    private SlurmRestPhysical slurmRestLocal;
    private HashMap<String, String> _jobID_PortMap = new HashMap<>();

    public DUUISlurmClusterInterface(SlurmRestPhysical rest_local) throws IOException {
        slurmRestLocal = rest_local;

    }

    public String run_json(DUUIPipelineComponent component, int hostPort) {

        return "";
    }

 // return 1.jobid  2.nodename
    public List<String> run_batch(DUUIPipelineComponent component, int hostPort, int i) throws Exception {
        JobConfig jobConfig = slurmRestLocal.getReader().loadConfig();
        String serverPath = jobConfig.getServer();
        String password = jobConfig.getPassword();
        String sshPort = jobConfig.getSshPort();
        String username = jobConfig.getUsername();
        String job = jobConfig.getJobs().get(0).replace("PLACEHOLDER", Integer.toString(hostPort));

        String serverWorkdir  = jobConfig.getWorkDir();
        String filename = "src/main/resources/slurmDriverConf/"+Integer.toString(i)+".sh";
        SlurmUtils.saveStrAsFile(job, filename);

        SlurmSSHBatch.SSHHelper.uploadScriptFile(serverPath, username, password, filename,serverWorkdir );

        String id = SlurmSSHBatch.SSHHelper.submitMultipleJobs(serverPath, username, password, List.of(job)).get(0);
        String s = SlurmSSHBatch.SSHHelper.waitForNodeAllocationSshj(serverPath, username, password, id, 120);
        _jobID_PortMap.put(id, Integer.toString(hostPort));
        return  List.of(id,s);

    }




    public String run(DUUIPipelineComponent component, int hostPort) {
        //TODO here for rest submit and return id back
        /*
        {
  "jobs": [
    {
      "job_id": xxxx,
      "job_state": [
        "RUNNING"
      ],
      "nodes": "node36-002",
      "cpus_allocated": 4,
    }
  ],
  "errors": []
}
         */
        // then port forwarding
        return "";
    }

    public String extractPort(String jobId) {
        return "";
    }

    public HashMap<String, String> get_jobID_PortMap() {
        return _jobID_PortMap;
    }





}
