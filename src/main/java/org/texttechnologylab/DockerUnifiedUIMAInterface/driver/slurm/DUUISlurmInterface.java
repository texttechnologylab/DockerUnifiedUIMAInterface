package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;

import java.io.IOException;
import java.util.HashMap;


public class DUUISlurmInterface {

    private SlurmRestContainer slurmRestContainer;
    private SlurmRestPhysical slurmRestLocal;
    private AbstractSlurm abstractSlurm;
    private HashMap<String, String> _jobID_PortMap = new HashMap<>();

    public DUUISlurmInterface(SlurmRestContainer rest) throws IOException {
        slurmRestContainer = rest;
        abstractSlurm = slurmRestContainer;

    }
    public DUUISlurmInterface(SlurmRestPhysical rest_local) throws IOException {
        slurmRestLocal = rest_local;
        abstractSlurm = slurmRestLocal;
    }


    /**
     * Generate a script based on the parameters given by the user, submit and execute it
     * @param comp
     * @param hostPort
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String run(DUUIPipelineComponent comp, int hostPort) throws IOException, InterruptedException {
        String hport = Integer.toString(hostPort);
        String gpu = comp.getSlurmGPU();
        String sifName = comp.getSlurmSIFImageName();
        String slurmJobName = comp.getSlurmJobName();
        String slurmRuntime = comp.getSlurmRuntime();
        String slurmMem = comp.getSlurmMem();
        String slurmSaveIn = comp.getSlurmSIFLocation();
        String slurmUvicorn = comp.getSlurmUvicorn();
        String slurmNodelist = comp.getSlurmNodelist();
        String slurmPartition = comp.getSlurmPartition();
        String slurmWorkDir = comp.getSlurmWorkDir();
        String slurmCpus = comp.getSlurmCpus();
        JSONObject j = new JSONObject();
        if (Integer.parseInt(gpu)>0){
            JSONObject jsonObject = SlurmUtils.generateJobScript_GPU(slurmJobName, slurmRuntime, slurmWorkDir, hport, slurmSaveIn, slurmUvicorn, slurmMem, gpu, sifName, slurmPartition, slurmNodelist, slurmCpus);
            j = jsonObject;
        }
        else {
            j = SlurmUtils.generateJobScript_non_GPU(slurmJobName,slurmRuntime,slurmWorkDir,hport,slurmSaveIn, slurmUvicorn,slurmMem,sifName,slurmPartition,slurmNodelist, slurmCpus);

        }
        String token = "";
        String[] submit = new String[2];
        if (slurmRestLocal == null) {
            System.out.println("[SlurmDriver] slurm in Docker mode activated");
            token = slurmRestContainer.generateRootToken("slurmctld");
            submit = slurmRestContainer.submit(j);
        }
        if (slurmRestContainer == null){
            System.out.println("[SlurmDriver] normal slurm mode activated");
            if(!slurmRestLocal.getPassword_token().isEmpty()){
            token = slurmRestLocal.getPassword_token();
            }
            else{
                token = slurmRestLocal.generateTokenByHost();
            }
            System.out.println("THIS CODE should given by admin");
            submit = slurmRestLocal.submit(j);
        }


        
        //System.out.println(submit[0]);
        String id = submit[1];
        // System.out.println("[SlurmDriver] Job submitted , jobid : " + id);
        _jobID_PortMap.put(id, Integer.toString(hostPort));

        return id;
    }

    /**
     * submit and execute json script
     * @param comp
     * @param hostPort
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public String run_json(DUUIPipelineComponent comp, int hostPort) throws IOException, InterruptedException {
        String slurmScript = comp.getSlurmScript();
        JSONObject slurmJson = new JSONObject(slurmScript);
        if (abstractSlurm.checkRESTD()) {
            String token = "";
            if (abstractSlurm instanceof SlurmRestContainer){
                token = slurmRestContainer.generateRootToken("slurmctld");
            }
            else{
                token = slurmRestLocal.generateTokenByHost();
            }
            String[] submit = abstractSlurm.submit(slurmJson);
            //System.out.println(submit[0]);
            _jobID_PortMap.put(submit[1], Integer.toString(hostPort));
            return submit[1];
        }
        else {
            throw new RuntimeException("slurm rest check failed");
        }
    }


    public String extractPort(String jobid) {
        return _jobID_PortMap.get(jobid);
    }

    public HashMap<String, String> get_jobID_PortMap() {
        return _jobID_PortMap;
    }

}

