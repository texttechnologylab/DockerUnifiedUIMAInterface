import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Paragraph;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.annotation.semaf.isobase.Entity;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NegLab {

    @Test
    public void FReND() throws URISyntaxException, IOException {

//        URI sURI = new URI("https://raw.githubusercontent.com/lattice-8094/FReND/main/my_xml_corpus.xml");

//        File pFile = FileUtils.downloadFile(sURI.toString());
//        pFile.deleteOnExit();

        File pFile = new File("/home/staff_homes/abrami/Downloads/my_xml_corpus.xml");
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
                                sb.append(textNode);

                                break;

                        }
                    }
                    System.out.println(sAnnotations);

                    sbGlobal.append(sb.toString());
                    int iStart = sbGlobal.indexOf(sb.toString());
                    int iEnd = iStart + sb.toString().length();
                    sParagraph.add(iStart + "|" + iEnd);
                    System.out.println("stop");

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
                }


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
