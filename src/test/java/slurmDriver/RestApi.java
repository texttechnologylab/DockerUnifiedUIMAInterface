package slurmDriver;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmUtils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.slurmInDocker.SlurmRest;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class RestApi {
    SlurmRest slurmRest = new SlurmRest("http:localhost");

    @Test
    public void dockerCommandTest() throws IOException {
        boolean b = slurmRest.checkRESTD();
        System.out.println(b);
        System.out.println(slurmRest.showHostName());
        List<String> strings = slurmRest.listContainerNames();
        System.out.println(strings);

    }

    @Test
    public void seeInfos() throws IOException, InterruptedException {
        //    GET /slurm/v0.0.42/diag
        //    GET /slurm/v0.0.42/nodes
        //    GET /slurm/v0.0.42/jobs

        String s = slurmRest.generateRootToken("compute1");
        System.out.println("=============================================================================");
        System.out.println(slurmRest.query(s, "diag"));
        System.out.println("=============================================================================");
        System.out.println(slurmRest.query(s, "jobs"));
        System.out.println("=============================================================================");


    }

    @Test
    public void splitToken() {
        String token = "SLURM_JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTE4ODY3MzIsImlhdCI6MTc1MTg4NDkzMiwic3VuIjoiamQifQ.uzplyXbsDPPix1hEU7JNcUhWUm8YPs6UHNTUpODLyE0";
        String[] split = token.split("=", 2);
        split[0] = split[0].trim();
        split[1] = split[1].trim();
        System.out.println(split[0]);
        System.out.println(split[1]);
    }

    @Test
    public void submitByRoot() throws IOException, InterruptedException {
        JSONObject job = new JSONObject()
                .put("name", "rest_smoke")
                .put("partition", "normal")
                .put("time_limit", 300)
                .put("current_working_directory", "/data")
                .put("environment", new JSONArray().put("PATH=/bin/:/usr/bin/:/sbin/"));

        JSONObject payload = new JSONObject()
                .put("script", "#!/bin/bash\nsrun hostname")
                .put("job", job);

        String token = slurmRest.generateRootToken("compute1");
        Arrays.stream(slurmRest.submit(token, payload)).forEach(System.out::println);
    }

    @Test
    public void seeJobIDn() throws IOException, InterruptedException {
        String s = slurmRest.generateRootToken("compute1");
        System.out.println("=============================================================================");
        System.out.println(slurmRest.query(s, "job/13"));
    }

    @Test
    public void submitSpacy() throws IOException, InterruptedException {

        JSONObject job = new JSONObject()
                .put("name", "spacy_test")
                .put("partition", "normal")
                .put("cpus_per_task", 2)
                .put("required_nodes", List.of("compute1"))
                .put( "tres_per_node", "gres/gpu=1")
                .put("memory_per_node", new JSONObject()
                        .put("set", true)
                        .put("number", 2048))
                .put("time_limit", 600)//minute
                .put("current_working_directory", "/data")
                .put("environment", new JSONArray().put("PATH=/bin/:/usr/bin/:/sbin/"));

        JSONObject payload = new JSONObject()
                .put("script",
                        "#!/bin/bash\n" +
                                "PORT=20000 \n" +
                                "INNER=9714 \n" +
                                "IMG=\"/sif_pool/spacy.sif\"  " +
                                "INTOIMAGE=\"cd /usr/src/app\"\n" +
                                "UVI=\"uvicorn textimager_duui_spacy:app\"   \n" +
                                "\n" +
                                "apptainer exec \"$IMG\" \\\n" +
                                "  sh -c \"$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER\" &\n" +
                                "\n" +
                                "PID=$!\n" +
                                "\n" +
                                "socat TCP-LISTEN:$PORT,reuseaddr,fork TCP:127.0.0.1:$INNER &\n" +
                                "\n" +
                                "PID_SOCAT=$!\n" +
                                "\n" +
                                "trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT\n" +
                                "\n" +
                                "wait $PID")
                .put("job", job);

        String token = slurmRest.generateRootToken("compute1");
        Arrays.stream(slurmRest.submit(token, payload)).forEach(System.out::println);
    }

    @Test
    public void cancelJob() throws IOException, InterruptedException {
        String token = slurmRest.generateRootToken("compute1");
        assert slurmRest.cancelJob(token, "5");

    }

    @Test
    public void runParlbert() throws IOException, InterruptedException {
        JSONObject job = new JSONObject()
                .put("name", "cuda_test")
                .put("partition", "normal")
                .put("time_limit", 600)
                .put("required_nodes", "compute1")
                .put("cpus_per_task", 2)
                .put("current_working_directory", "/data")
                .put("environment", new JSONArray().put("PATH=/bin/:/usr/bin/:/sbin/"));

        JSONObject payload = new JSONObject()
                .put("script",
                        "#!/bin/bash\n" +
                                "#SBATCH --gres=gpu:1\n" +
                                "#SBATCH --partition=normal\n" +
                                "#SBATCH --nodelist=compute1\n" +

                                "PORT=20000 \n" +
                                "INNER=9714 \n" +
                                "IMG=\"/sif_pool/bert_cuda.sif\"  " +
                                "INTOIMAGE=\"cd /usr/src/app\"\n" +
                                "UVI=\"uvicorn parlbert_topic_german:app\"   \n" +
                                "\n" +
                                "apptainer exec --nv \"$IMG\" \\\n" +
                                "  sh -c \"$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER\" &\n" +
                                "\n" +
                                "PID=$!\n" +
                                "\n" +
                                "socat TCP-LISTEN:$PORT,reuseaddr,fork TCP:127.0.0.1:$INNER &\n" +
                                "\n" +
                                "PID_SOCAT=$!\n" +
                                "\n" +
                                "trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT\n" +
                                "\n" +
                                "wait $PID")
                .put("job", job);
        System.out.println(payload.toString());
        //String token = slurmRest.generateRootToken("compute1");
        //Arrays.stream(slurmRest.submit(token, payload)).forEach(System.out::println);
    }

    @Test
    public void runNvidiasmi() throws IOException, InterruptedException {
        JSONObject job = new JSONObject()
                .put("name", "smi_test")
                .put("partition", "normal")
                .put("time_limit", 600)
                .put("current_working_directory", "/data")
                .put("environment", new JSONArray().put("PATH=/bin/:/usr/bin/:/sbin/"));

        JSONObject payload = new JSONObject()
                .put("script",
                        "#!/bin/bash\n#SBATCH --gres=gpu:1\nnvidia-smi")
                .put("job", job);

        String token = slurmRest.generateRootToken("compute1");
        Arrays.stream(slurmRest.submit(token, payload)).forEach(System.out::println);
    }


    @Test
    public void pipelineTest_json() throws Exception {
        SlurmRest rest = new SlurmRest("http://localhost");
        String token = rest.generateRootToken("compute1");

        int iWorkers = 1; //
        JCas jc = JCasFactory.createText("Early life\n" +
                "William Gordon Gordon-Cumming was born on 20 July 1848 at Sanquhar House, near Forres, Morayshire.[1] His parents were Alexander Penrose Gordon-Cumming—the third of the Gordon-Cumming baronets—and Anne Pitcairn (née Campbell). William was the second of the couple's four children and their eldest son. His uncle, Roualeyn George Gordon-Cumming, was a noted big-game hunter; and his aunt, Constance Gordon-Cumming, was a travel writer. Gordon-Cumming was educated at the English boarding schools Eton and Wellington.[1][2]\n" +
                "\n" +
                "At the age of eighteen he inherited the baronetcy and became chief of Clan Cumming; his line had been traced from the fourth century, through Charlemagne. His inheritance included three Morayshire estates: Altyre near Forres, Gordonstoun near Elgin, and the village of Dallas. The estates totalled 38,500 acres (156 km2) of poor quality land;[1][3] the annual income from the estates in around 1890 has been given as either £60,000[4] or £80,000.[5][a]\n" +
                "\n" +
                "Military career\n" +
                "Although Gordon-Cumming had asthma and was blind in one eye, he purchased a commission as ensign in the Scots Fusilier Guards (renamed in 1877 as the Scots Guards) in 1868 (dated from 25 December 1867).[1][7][8] He was promoted to regimental lieutenant and to the brevet rank of captain in the army by purchase on 17 May 1871, the last year in which commissions were available for purchase.[9][b] He volunteered for service in South Africa in the Anglo-Zulu War, where he served gallantly and was mentioned in despatches; he was the first man to enter Cetshwayo's kraal after the Battle of Ulundi (1879). That year he conveyed the condolences of the army to the ex-Empress Eugénie on the death of her son, Napoléon, Prince Imperial.[1][2]\n" +
                "\n" +
                "Gordon-Cumming was promoted to the regimental rank of captain and the army rank of lieutenant-colonel on 28 July 1880.[11] He served in Egypt during the Anglo-Egyptian War (1882) and in the Sudan in the Mahdist War (1884–1885), the last of which was with the Guards Camel Regiment in the Desert Column.[2][4][c] He was promoted to the regimental rank of major on 23 May 1888.[13]\n" +
                "\n" +
                "He also found time for independent travel and adventure, stalking tigers on foot in India and hunting in the Rocky Mountains in the US;[1][4] in 1871 he published an account of his travels in India, Wild Men and Wild Beasts: Scenes in Camp and Jungle.[14] The work covers the best routes to travel to and from India and which animals are available for hunting in which season, as well as the equipment a hunter would need to take on an expedition.[15] He concluded his work with the following:\n" +
                "\n" +
                "The record of my doings might no doubt have been more acceptable to the general reader had it been more varied with matter other than mere slaughter, and had the tale of bloodshed been more frequently relieved by accounts of the geography, scenery, and natural history, human and bestial, of the country; but all these have been well described elsewhere, and by abler pens.[16]\n" +
                "\n" +
                "Royal baccarat scandal\n" +
                "Main article: Royal baccarat scandal\n" +
                "In September 1890 Arthur Wilson, the 52-year-old Hull-based owner of a shipping business, invited Gordon-Cumming, along with Edward, Prince of Wales, to a house party at Tranby Croft in the East Riding of Yorkshire;[17][18] Gordon-Cumming and the prince had been friends for over twenty years.[19] Among the other people present that weekend were Wilson's wife, Mary, their son, Stanley, their daughter, Ethel, and her husband, Edward Lycett Green, who was the son of Sir Edward Green, 1st Baronet, a local Conservative politician.[20] Several members of the prince's inner circle were also invited to stay, including Sir Christopher Sykes—the Conservative MP for Beverley—the equerry Tyrwhitt Wilson, Lord Coventry, Lord Edward Somerset, Captain Arthur Somerset—his cousin—and Lieutenant-General Owen Williams, along with their wives. Also accompanying the party was Lieutenant Berkeley Levett, a brother officer to Gordon-Cumming in the Scots Guards and a friend of the Wilson family.[21]\n" +
                "\n" +
                "During the evenings of the weekend, Edward insisted on playing baccarat, a game that was at the time illegal if gambling was involved;[22] many of the house joined in, including Gordon-Cumming, Levett and Stanley Wilson. The prince acted as the dealer.[23][d] On the first night of play, Stanley Wilson thought he saw Gordon-Cumming add two red £5 counters onto his stake after the hand had finished, but before the winnings had been paid, thus increasing the money paid to him by the bank—a method of cheating known in casinos as la poussette. He alerted Levett, sitting next to him, and both men thought they saw Gordon-Cumming repeat the act on the next hand.[24][25]\n" +
                "\n" +
                "After the second evening of play Lycett Green, Stanley Wilson and Arthur and Edward Somerset confronted Gordon-Cumming and accused him of cheating.[18][e] Gordon-Cumming insisted they had been mistaken, and explained that he played the coup de trois system of betting,[f] in which if he won a hand with a £5 stake, he would add his winnings to the stake, together with another £5, as the stake for the next hand.[28][g] Edward, after hearing from his advisors and the accusers, believed what they had told him.[29] In order to avoid a scandal involving the prince, Gordon-Cumming gave way to pressure from the attendant royal courtiers to sign a statement undertaking never to play cards again in return for a pledge that no-one present would speak of the incident to anyone else.[30][31]\n" +
                "\n" +
                "In consideration of the promise made by the gentlemen whose names are subscribed to preserve my silence with reference to an accusation which has been made in regard to my conduct at baccarat on the nights of Monday and Tuesday the 8th and 9th at Tranby Croft, I will on my part solemnly undertake never to play cards again as long as I live.\n" +
                "\n" +
                "— (Signed) W. Gordon-Cumming[32]");
        DocumentMetaData dmd = new DocumentMetaData(jc);
        dmd.setDocumentId("test_rest");
        dmd.setDocumentTitle("test_REST");
        dmd.addToIndexes();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUILuaSandbox sandbox = new DUUILuaSandbox();
        sandbox._allowAllJavaClasses = true;
        ctx.withSandbox(sandbox);
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUISlurmDriver slurmDriver = new DUUISlurmDriver(rest);

        composer.addDriver(uimaDriver, slurmDriver);

        composer.resetPipeline();
        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(
                new DUUIPipelineComponent().withSlurmScript(
                        "{\"job\":{\"environment\":[\"PATH=/bin/:/usr/bin/:/sbin/\"],\"partition\":\"normal\",\"time_limit\":600,\"name\":\"spacy_test\",\"current_working_directory\":\"/data\"},\"script\":\"#!/bin/bash\\n \\nPORT=20000 \\nINNER=9714 \\nIMG=\\\"/sif_pool/spacy.sif\\\"  INTOIMAGE=\\\"cd /usr/src/app\\\"\\nUVI=\\\"uvicorn textimager_duui_spacy:app\\\"   \\n\\napptainer exec --nv \\\"$IMG\\\" \\\\\\n  sh -c \\\"$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER\\\" &\\n\\nPID=$!\\n\\nsocat TCP-LISTEN:$PORT,reuseaddr,fork TCP:127.0.0.1:$INNER &\\n\\nPID_SOCAT=$!\\n\\ntrap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT\\n\\nwait $PID\"}\n"
                ).withSlurmHostPort("20000")).build();

        System.out.println(com1.getSlurmScript());

        composer.add(com1);
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/nlp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());

        composer.run(jc);


    }

    @Test
    public void pipelineTest_parameters() throws Exception {
        SlurmRest rest = new SlurmRest("http://localhost");


        int iWorkers = 2; //
        JCas jc = JCasFactory.createText("Early life\n" +
                "William Gordon Gordon-Cumming was born on 20 July 1848 at Sanquhar House, near Forres, Morayshire.[1] His parents were Alexander Penrose Gordon-Cumming—the third of the Gordon-Cumming baronets—and Anne Pitcairn (née Campbell). William was the second of the couple's four children and their eldest son. His uncle, Roualeyn George Gordon-Cumming, was a noted big-game hunter; and his aunt, Constance Gordon-Cumming, was a travel writer. Gordon-Cumming was educated at the English boarding schools Eton and Wellington.[1][2]\n" +
                "\n" +
                "At the age of eighteen he inherited the baronetcy and became chief of Clan Cumming; his line had been traced from the fourth century, through Charlemagne. His inheritance included three Morayshire estates: Altyre near Forres, Gordonstoun near Elgin, and the village of Dallas. The estates totalled 38,500 acres (156 km2) of poor quality land;[1][3] the annual income from the estates in around 1890 has been given as either £60,000[4] or £80,000.[5][a]\n" +
                "\n" +
                "Military career\n" +
                "Although Gordon-Cumming had asthma and was blind in one eye, he purchased a commission as ensign in the Scots Fusilier Guards (renamed in 1877 as the Scots Guards) in 1868 (dated from 25 December 1867).[1][7][8] He was promoted to regimental lieutenant and to the brevet rank of captain in the army by purchase on 17 May 1871, the last year in which commissions were available for purchase.[9][b] He volunteered for service in South Africa in the Anglo-Zulu War, where he served gallantly and was mentioned in despatches; he was the first man to enter Cetshwayo's kraal after the Battle of Ulundi (1879). That year he conveyed the condolences of the army to the ex-Empress Eugénie on the death of her son, Napoléon, Prince Imperial.[1][2]\n" +
                "\n" +
                "Gordon-Cumming was promoted to the regimental rank of captain and the army rank of lieutenant-colonel on 28 July 1880.[11] He served in Egypt during the Anglo-Egyptian War (1882) and in the Sudan in the Mahdist War (1884–1885), the last of which was with the Guards Camel Regiment in the Desert Column.[2][4][c] He was promoted to the regimental rank of major on 23 May 1888.[13]\n" +
                "\n" +
                "He also found time for independent travel and adventure, stalking tigers on foot in India and hunting in the Rocky Mountains in the US;[1][4] in 1871 he published an account of his travels in India, Wild Men and Wild Beasts: Scenes in Camp and Jungle.[14] The work covers the best routes to travel to and from India and which animals are available for hunting in which season, as well as the equipment a hunter would need to take on an expedition.[15] He concluded his work with the following:\n" +
                "\n" +
                "The record of my doings might no doubt have been more acceptable to the general reader had it been more varied with matter other than mere slaughter, and had the tale of bloodshed been more frequently relieved by accounts of the geography, scenery, and natural history, human and bestial, of the country; but all these have been well described elsewhere, and by abler pens.[16]\n" +
                "\n" +
                "Royal baccarat scandal\n" +
                "Main article: Royal baccarat scandal\n" +
                "In September 1890 Arthur Wilson, the 52-year-old Hull-based owner of a shipping business, invited Gordon-Cumming, along with Edward, Prince of Wales, to a house party at Tranby Croft in the East Riding of Yorkshire;[17][18] Gordon-Cumming and the prince had been friends for over twenty years.[19] Among the other people present that weekend were Wilson's wife, Mary, their son, Stanley, their daughter, Ethel, and her husband, Edward Lycett Green, who was the son of Sir Edward Green, 1st Baronet, a local Conservative politician.[20] Several members of the prince's inner circle were also invited to stay, including Sir Christopher Sykes—the Conservative MP for Beverley—the equerry Tyrwhitt Wilson, Lord Coventry, Lord Edward Somerset, Captain Arthur Somerset—his cousin—and Lieutenant-General Owen Williams, along with their wives. Also accompanying the party was Lieutenant Berkeley Levett, a brother officer to Gordon-Cumming in the Scots Guards and a friend of the Wilson family.[21]\n" +
                "\n" +
                "During the evenings of the weekend, Edward insisted on playing baccarat, a game that was at the time illegal if gambling was involved;[22] many of the house joined in, including Gordon-Cumming, Levett and Stanley Wilson. The prince acted as the dealer.[23][d] On the first night of play, Stanley Wilson thought he saw Gordon-Cumming add two red £5 counters onto his stake after the hand had finished, but before the winnings had been paid, thus increasing the money paid to him by the bank—a method of cheating known in casinos as la poussette. He alerted Levett, sitting next to him, and both men thought they saw Gordon-Cumming repeat the act on the next hand.[24][25]\n" +
                "\n" +
                "After the second evening of play Lycett Green, Stanley Wilson and Arthur and Edward Somerset confronted Gordon-Cumming and accused him of cheating.[18][e] Gordon-Cumming insisted they had been mistaken, and explained that he played the coup de trois system of betting,[f] in which if he won a hand with a £5 stake, he would add his winnings to the stake, together with another £5, as the stake for the next hand.[28][g] Edward, after hearing from his advisors and the accusers, believed what they had told him.[29] In order to avoid a scandal involving the prince, Gordon-Cumming gave way to pressure from the attendant royal courtiers to sign a statement undertaking never to play cards again in return for a pledge that no-one present would speak of the incident to anyone else.[30][31]\n" +
                "\n" +
                "In consideration of the promise made by the gentlemen whose names are subscribed to preserve my silence with reference to an accusation which has been made in regard to my conduct at baccarat on the nights of Monday and Tuesday the 8th and 9th at Tranby Croft, I will on my part solemnly undertake never to play cards again as long as I live.\n" +
                "\n" +
                "— (Signed) W. Gordon-Cumming[32]");
        DocumentMetaData dmd = new DocumentMetaData(jc);
        dmd.setDocumentId("test_rest");
        dmd.setDocumentTitle("test_REST");
        dmd.addToIndexes();
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUILuaSandbox sandbox = new DUUILuaSandbox();
        sandbox._allowAllJavaClasses = true;
        ctx.withSandbox(sandbox);
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUISlurmDriver slurmDriver = new DUUISlurmDriver(rest);

        composer.addDriver(uimaDriver, slurmDriver);

        composer.resetPipeline();
        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(
                new DUUIPipelineComponent().
                        withSlurmPartition("normal").
                        withSlurmNodelist("compute1").
                        withSlurmWorkDir("/data").
                        withSlurmUvicorn("uvicorn textimager_duui_spacy:app").
                        withSlurmSIFName("spacy").
                        withSlurmMemory("1024").
                        withSlurmRuntime("5").withSlurmCPUs("2").
                        withSlurmSaveIn("/sif_pool/spacy.sif").withSlurmJobName("spacy").withSlurmCPUs("2").
                        withSlurmGPU("0").withScale(2)
        ).build();
        composer.add(com1);
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/nlp/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"))
                .build());
        composer.run(jc);

    }

}
