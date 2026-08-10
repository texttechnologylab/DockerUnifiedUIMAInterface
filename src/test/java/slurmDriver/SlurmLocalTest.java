package slurmDriver;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.io.xmi.XmiWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.DUUISlurmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.SlurmRestPhysical;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.ConfigManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm.config.SlurmConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaSandbox;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class SlurmLocalTest {
  String pass = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM5MzE2MjcxOTksImlhdCI6MTc4NDE0MzU1Mywic3VuIjoiamQifQ.iOH2wdmZB8ssbbFnHpIf-lYUtZ8gO4nfA5exlm3kbNM";

    SlurmRestPhysical sr = new SlurmRestPhysical(ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml")),pass);

    public SlurmLocalTest() throws IOException {
    }

    @Test
    public void showHostName(){
        try {
            System.out.println(sr.showHostName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void checkSlurmRestDaemon() throws IOException {
        boolean b = sr.checkRESTD();
        System.out.printf("status: %b", b);
    }

    @Test
    public void generateToken(){
        try {
            System.out.println(sr.generateTokenByHost());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /*
    where
"""/ping""",check api status
"""/jobs""",see all jobs
"""/nodes""",see nodes status/healthy
"""/diag""", see dignose

return string in json format
     */
    @Test
    public void query() throws IOException, InterruptedException {
        System.out.println(sr.query("nodes"));

    }

    @Test
    public void submitJob() throws IOException, InterruptedException {
        JSONObject job = new JSONObject()
                .put("name", "spacy_test")
                .put("partition", "normal")
                .put("cpus_per_task", 1)
                .put("required_nodes", List.of("jdSLURM"))
                .put( "tres_per_node", "gres/gpu=0")
                .put("memory_per_node", new JSONObject()
                        .put("set", true)
                        .put("number", 2048))
                .put("time_limit", 600)//minute
                .put("current_working_directory", "/home/jd")
                .put("environment", new JSONArray().put("PATH=/bin/:/usr/bin/:/sbin/"));

        JSONObject payload = new JSONObject()
                .put("script",
                        "#!/bin/bash\n" +
                                "port=20000 \n" +
                                "INNER=9714 \n" +
                                "IMG=\"/home/jd/CODE/spacy.sif\"  " +
                                "INTOIMAGE=\"cd /usr/src/app\"\n" +
                                "UVI=\"uvicorn textimager_duui_spacy:app\"   \n" +
                                "\n" +
                                "apptainer exec \"$IMG\" \\\n" +
                                "  sh -c \"$INTOIMAGE && $UVI --host 0.0.0.0 --port $INNER\" &\n" +
                                "\n" +
                                "PID=$!\n" +
                                "\n" +
                                "socat TCP-LISTEN:$port,reuseaddr,fork TCP:127.0.0.1:$INNER &\n" +
                                "\n" +
                                "PID_SOCAT=$!\n" +
                                "\n" +
                                "trap 'kill $PID $PID_SOCAT 2>/dev/null' EXIT\n" +
                                "\n" +
                                "wait $PID")
                .put("job", job);
        sr.submit( payload);


    }

    @Test
    public void cancelJob() throws IOException, InterruptedException {
        sr.cancelJob( "9");
    }


    @Test
    public void pipelineTest_parameters_cpu() throws Exception {
        SlurmRestPhysical rest_local = new SlurmRestPhysical(ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml")),pass);


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
        DUUISlurmDriver slurmDriver = new DUUISlurmDriver(rest_local);

        composer.addDriver(uimaDriver, slurmDriver);

        composer.resetPipeline();
        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(
                new DUUIPipelineComponent().
                        withSlurmPartition("normal").
                        withSlurmNodelist("jdSLURM").
                        withSlurmWorkDir("/home/jd/CODE").
                        withSlurmUvicorn("uvicorn textimager_duui_spacy:app").
                        withSlurmSIFName("spacy").
                        withSlurmMemory("1024").
                        withSlurmRuntime("5").withSlurmCPUs("1").
                        withSlurmSaveIn("/home/jd/CODE/spacy.sif").withSlurmJobName("spacy").
                        withSlurmGPU("0").withScale(1)
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




    @Test
    public void pipelineTest_parameters_gpu() throws Exception {
        SlurmRestPhysical rest_local = new SlurmRestPhysical(ConfigManager.getManager(new File("src/main/resources/slurmDriverConf/slurmDriver.yaml")),pass);


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
        DUUISlurmDriver slurmDriver = new DUUISlurmDriver(rest_local);

        composer.addDriver(uimaDriver, slurmDriver);

        composer.resetPipeline();
        DUUIPipelineComponent com1 = new DUUISlurmDriver.Component(
                new DUUIPipelineComponent().
                        withSlurmPartition("normal").
                        withSlurmNodelist("jdSLURM").
                        withSlurmWorkDir("/home/jd").
                        withSlurmUvicorn("uvicorn deberta_zero_shot:app").
                        withSlurmSIFName("deberta").
                        withSlurmMemory("8000").
                        withSlurmRuntime("5").withSlurmCPUs("1").
                        withSlurmSaveIn("/home/jd/CODE/deberta.sif").withSlurmJobName("deberta").
                        withSlurmGPU("1").withScale(1)
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
