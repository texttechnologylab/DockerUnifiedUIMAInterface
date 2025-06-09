package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import java.util.HashMap;
@Deprecated
public class SlurmConfig {
//    #!/bin/bash
//#SBATCH --job-name=duui
//#SBATCH --cpus-per-task=2
//            #SBATCH --time=1:00:00
//
//    PORT=9000
//    INNER=9714
//    IMG=/home/jd/again.sif
//
//    apptainer exec "$IMG" \
//    sh -c "cd /usr/src/app && uvicorn textimager_duui_spacy:app --host 0.0.0.0 --port $INNER" &
//
//    PID=$!
//    socat TCP-LISTEN:$PORT,reuseaddr,fork TCP:127.0.0.1:$INNER &
//    PID_SOCAT=$!
//
//    wait $PID
//    kill $PID_SOCAT

    private static String slurmJobName = "slurmJobName";
    private static String slurmImagePort = "slurmPort";
    private static String slurmHostPort = "slurmHostPort";
    private static String slurmRuntime = "slurmRuntime";
    private static String slurmCpus = "slurmCpus";
    private static String slurmMemory = "slurmMemory";
    private static String slurmErrorLocation = "slurmErrorLocation";
    private static String slurmOutPutLocation = "slurmOutPutLocation";
    private static String slurmSIFSaveIn = "slurmSIFSaveIn";
    private static String slurmGPU = "slurmGPU";
    private static String slurmSIFImageName = "slurmSIFImageName";
    private static String slurmEntryLocation = "slurmEntryLocation";


    private String slurmJobNameV;
    private String slurmImagePortV;
    private String slurmHostPortV;
    private String slurmRuntimeV;
    private String slurmCpusV;
    private String slurmMemoryV;
    private String slurmErrorLocationV;
    private String slurmOutPutLocationV;
    private String slurmSIFSaveInV;
    private String slurmGPUV;
    private String slurmSIFImageNameV;
    private String entryLocationV;
    private HashMap<String, String> slurmConfigProp = new HashMap<String, String>();


    public SlurmConfig(String slurmJobNameV, String slurmImagePortV, String slurmHostPortV, String slurmRuntimeV, String slurmCpusV, String slurmMemoryV, String slurmErrorLocationV, String slurmOutPutLocationV, String slurmSIFSaveInV, String slurmGPUV, String slurmSIFImageNameV, String entryLocationV) {
        this.slurmJobNameV = slurmJobNameV;
        this.slurmImagePortV = slurmImagePortV;
        this.slurmHostPortV = slurmHostPortV;
        this.slurmRuntimeV = slurmRuntimeV;
        this.slurmCpusV = slurmCpusV;
        this.slurmMemoryV = slurmMemoryV;
        this.slurmErrorLocationV = slurmErrorLocationV;
        this.slurmOutPutLocationV = slurmOutPutLocationV;
        this.slurmSIFSaveInV = slurmSIFSaveInV;
        this.slurmGPUV = slurmGPUV;
        this.slurmSIFImageNameV = slurmSIFImageNameV;
        this.entryLocationV = entryLocationV;
       //----------------------------------------------------------------
        slurmConfigProp.put(slurmJobName, slurmJobNameV);
        slurmConfigProp.put(slurmImagePort, slurmImagePortV);
        slurmConfigProp.put(slurmHostPort, slurmHostPortV);
        slurmConfigProp.put(slurmRuntime, slurmRuntimeV);
        slurmConfigProp.put(slurmCpus, slurmCpusV);
        slurmConfigProp.put(slurmMemory, slurmMemoryV);
        slurmConfigProp.put(slurmErrorLocation, slurmErrorLocationV);
        slurmConfigProp.put(slurmOutPutLocation, slurmOutPutLocationV);
        slurmConfigProp.put(slurmSIFSaveIn, slurmSIFSaveInV);
        slurmConfigProp.put(slurmGPU, slurmGPUV);
        slurmConfigProp.put(slurmSIFImageName, entryLocationV);
        slurmConfigProp.put(slurmEntryLocation, entryLocationV);

    }


}
