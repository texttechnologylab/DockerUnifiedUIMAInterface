import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReaderLazy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.CountAnnotations;
import org.texttechnologylab.annotation.semaf.isobase.Entity;
import org.texttechnologylab.annotation.semaf.semafsr.SrLink;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class NegLab {


    @Test
    public void countAnnotations() throws Exception {

        int iWorker = 20;

        String sInputPath = "";
        String sOutputPath = "/tmp/counting/";
        String sName = "Gutenberg"+".json";
        String sSuffix = "xmi.gz";



        DUUICollectionReader pReader = new DUUIFileReaderLazy(sInputPath, sSuffix, 10);
        // Asynchroner reader für die Input-Dateien
        DUUIAsynchronousProcessor pProcessor = new DUUIAsynchronousProcessor(pReader);
        new File(sOutputPath).mkdir();

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorker);         // wir geben dem Composer eine Anzahl an Threads mit.

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(docker_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.


        composer.add(new DUUIUIMADriver.Component(createEngineDescription(CountAnnotations.class,
                CountAnnotations.PARAM_TARGET_LOCATION, sOutputPath)).build());

        composer.run(pProcessor, "counting");
    }

    @Test
    public void FReND() throws URISyntaxException, IOException {

//        URI sURI = new URI("https://raw.githubusercontent.com/lattice-8094/FReND/main/my_xml_corpus.xml");

//        File pFile = FileUtils.downloadFile(sURI.toString());
//        pFile.deleteOnExit();

        File pFile = new File("/home/gabrami/Downloads/my_xml_corpus.xml");
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try {

            // optional, but recommended
            // process XML securely, avoid attacks like XML External Entities (XXE)
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

            // parse XML file
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.parse(pFile);

            NodeList annotations = doc.getElementsByTagName("Document");
            JCas tCas = JCasFactory.createJCas();

            StringBuilder sbGlobal = new StringBuilder();

            Set<String> sParagraph = new HashSet<>(0);
            Set<String> sAnnotations = new HashSet<>(0);

            for (int a = 0; a < annotations.getLength(); a++) {

                Node pDocumentNode = annotations.item(a);

                List<Node> pParts = getNodesFromXML(pDocumentNode, "DocumentPart");
                String sDocID = getSingleNodesFromXML(pDocumentNode, "DocID").getTextContent();
                System.out.println(sDocID);

                for (Node pPart : pParts) {

                    NodeList children = pPart.getChildNodes();
                    StringBuilder sb = new StringBuilder();

                    for (int c = 0; c < children.getLength(); c++) {
                        Node tNode = children.item(c);

                        switch (tNode.getNodeName()) {

                            case "Text":
                                if (tNode.getParentNode().getNodeName().equalsIgnoreCase("DocumentPart")) {
                                    sb.append(tNode.getTextContent());
                                }
                                break;

                            case "Cue":
                            case "Scope":
                                String sRef = tNode.getAttributes().getNamedItem("ref").getTextContent();
                                String sID = tNode.getAttributes().getNamedItem("id").getTextContent();
                                Node textNode = getSingleNodesFromXML(tNode, "Text");
                                sAnnotations.add(sID + "|" + tNode.getNodeName() + "|" + (sbGlobal.length() + sb.length()) + "|" + (sbGlobal.length() + sb.length() + textNode.getTextContent().length()) + "|" + sRef);
                                sb.append(textNode.getTextContent());

                                break;

                        }
                    }

                    sbGlobal.append(sb.toString());
                    int iStart = sbGlobal.indexOf(sb.toString());
                    int iEnd = iStart + sb.toString().length();
                    sParagraph.add(iStart + "|" + iEnd);
                }

                tCas.setDocumentText(sbGlobal.toString());
                tCas.setDocumentLanguage("fr");

                for (String s : sParagraph) {
                    String[] pSplit = s.split("\\|");
                    int iStart = Integer.parseInt(pSplit[0]);
                    if (iStart < 0) {
                        iStart = 0;
                    }
                    int iEnd = Integer.parseInt(pSplit[1]);
                    Paragraph p = new Paragraph(tCas, iStart, iEnd);
                    p.addToIndexes();
                }

                Map<String, Map<String, Set<Entity>>> scopeSet = new HashMap<>();
                Map<String, Map<String, Set<Entity>>> cueSet = new HashMap<>();

                for (String sAnnotation : sAnnotations) {
                    String[] pSplit = sAnnotation.split("\\|");
                    System.out.println("Stop");
                    int iStart = Integer.parseInt(pSplit[2]);
                    if (iStart < 0) {
                        iStart = 0;
                    }
                    int iEnd = Integer.parseInt(pSplit[3]);
                    Entity nEntity = new Entity(tCas, iStart, iEnd);
                    nEntity.addToIndexes();
                    String sKey = pSplit[0].substring(0, pSplit[0].lastIndexOf("_"));
                    if(pSplit[1].equalsIgnoreCase("cue")){
                        Map<String, Set<Entity>> eSet = new HashMap<>(0);
                        Set<Entity> entSet = new HashSet<>(0);
                        if(cueSet.containsKey(sKey)){
                            eSet = cueSet.get(sKey);
                            entSet = eSet.get(pSplit[4]);
                            if(entSet==null){
                                entSet = new HashSet<>(0);
                            }
                        }
                        entSet.add(nEntity);
                        eSet.put(pSplit[4], entSet);
                        cueSet.put(sKey, eSet);
                    }
                    else{
                        Map<String, Set<Entity>> eSet = new HashMap<>(0);
                        Set<Entity> entSet = new HashSet<>(0);
                        if(scopeSet.containsKey(sKey)){
                            eSet = scopeSet.get(sKey);
                            entSet = eSet.get(pSplit[4]);
                            if(entSet==null){
                                entSet = new HashSet<>(0);
                            }

                        }
                        entSet.add(nEntity);
                        eSet.put(pSplit[4], entSet);
                        scopeSet.put(sKey, eSet);
                    }
                }

                cueSet.keySet().stream().forEach(pCue->{

                    cueSet.get(pCue).keySet().forEach(eCue->{

                        if(scopeSet.containsKey(pCue)){
                            scopeSet.get(pCue).entrySet().stream().forEach(e->{
                                if(scopeSet.get(pCue).containsKey(e.getKey())){
                                    scopeSet.get(pCue).get(e.getKey()).stream().forEach(s->{
                                        cueSet.get(pCue).get(e.getKey()).stream().forEach(c->{

                                            long lTest = JCasUtil.select(tCas, SrLink.class).stream().filter(sr->{
                                                return sr.getFigure().equals(c) && sr.getGround().equals(s);
                                            }).count();
                                            if(lTest==0) {
                                                SrLink srLink = new SrLink(tCas);
                                                srLink.setGround(s);
                                                srLink.setFigure(c);
                                                srLink.setRel_type("Scope");
                                                srLink.addToIndexes();
                                            }
                                        });

                                    });
                                }
                            });
//                            scopeSet.get(eCue).stream().forEach(pScope->{
//
//                            });
                            cueSet.get(pCue).get(eCue).stream().forEach(c1->{
                                SrLink srLink = new SrLink(tCas);
                                srLink.setGround(c1);
                                srLink.setFigure(c1);
                                srLink.setRel_type("Cue");
                                srLink.addToIndexes();
                            });

                        }
                    });

                });

                DocumentMetaData dmd = new DocumentMetaData(tCas);
                dmd.setDocumentId("FReND");
                dmd.setDocumentTitle("FReND");
                dmd.addToIndexes();


//                CasIOUtils.save(tCas.getCas(), new FileOutputStream(new File("/tmp/FReND.xmi")), SerialFormat.XMI_1_1_PRETTY);

// docker.texttechnologylab.org/duui-spacy-fr_core_news_sm:0.4.3

                DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

                // Instanziierung des Composers, mit einigen Parametern
                DUUIComposer composer = new DUUIComposer()
                        .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                        .withLuaContext(ctx)            // wir setzen den definierten Kontext
                        .withWorkers(1);         // wir geben dem Composer eine Anzahl an Threads mit.

                DUUIDockerDriver docker_driver = new DUUIDockerDriver();
                DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                        .withDebug(true);

                // Hinzufügen der einzelnen Driver zum Composer
                composer.addDriver(docker_driver, uima_driver);  // remote_driver und swarm_driver scheint nicht benötigt zu werden.


                composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/duui-spacy-fr_core_news_sm:0.4.3")
                        .withScale(1)
                        .build());

                composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, "/tmp/",
                        XmiWriter.PARAM_PRETTY_PRINT, true,
                        XmiWriter.PARAM_OVERWRITE, true,
                        XmiWriter.PARAM_VERSION, "1.1"//,
//                        XmiWriter.PARAM_COMPRESSION, "GZIP"
                )).build());

                composer.run(tCas, "spacy");
//                NodeList pChildNodes = pDocumentNode.getChildNodes();
//
//                for(int b=0; b < pChildNodes.getLength(); b++){
//
//                    Node pChild = pChildNodes.item(b);
//                    NamedNodeMap attributes = pChild.getAttributes();
////                    switch (pChild.getNodeName()){
////                        case "DocID":
////                            System.out.println("Document: "+pChild.getTextContent());
////                            break;
////
////                        case "DocumentPart":
////                            System.out.println(attributes.getNamedItem("number_part_of_document").getTextContent());
////                            break;
////
////                        case "Cue":
////                            System.out.println(attributes.getNamedItem("type").getTextContent());
////                            System.out.println(attributes.getNamedItem("R1").getTextContent());
////                            System.out.println("\t"+pChild.getTextContent());
////                            break;
////
////                        case "Scope":
////                            System.out.println(attributes.getNamedItem("R1").getTextContent());
////                            System.out.println("\t"+pChild.getTextContent());
////                            break;
////                    }
//
//                    System.out.println(pChildNodes.item(b).getNodeName());
//                }
            }

        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            throw new RuntimeException(e);
        } catch (ResourceInitializationException e) {
            throw new RuntimeException(e);
        } catch (CASException e) {
            throw new RuntimeException(e);
        } catch (CompressorException e) {
            throw new RuntimeException(e);
        } catch (InvalidXMLException e) {
            throw new RuntimeException(e);
        } catch (UIMAException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * List all nodes based on a tag-name as part of a given node
     * @param pNode
     * @param sNodeName
     * @return
     */
    public static List<Node> getNodesFromXML(Node pNode, String sNodeName){

        List<Node> rSet = new ArrayList<>(0);

        if(pNode.getNodeName().equals(sNodeName)) {
            rSet.add(pNode);

        }
        else{

            if (pNode.hasChildNodes()) {
                for (int a = 0; a < pNode.getChildNodes().getLength(); a++) {
                    rSet.addAll(getNodesFromXML(pNode.getChildNodes().item(a), sNodeName));
                }
            } else {
                if (pNode.getNodeName().equals(sNodeName)) {
                    rSet.add(pNode);
                }
            }
        }

        return rSet;

    }

    /**
     * Get one node on a tag-name as part of a given node
     * @param pNode
     * @param sNodeName
     * @return
     */
    public static Node getSingleNodesFromXML(Node pNode, String sNodeName){

        List<Node> nList = getNodesFromXML(pNode, sNodeName);

        if(nList.size()>0){
            return nList.stream().findFirst().get();
        }
        return null;

    }

}
