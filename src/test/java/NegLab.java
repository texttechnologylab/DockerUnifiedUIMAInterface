import org.junit.jupiter.api.Test;
import org.texttechnologylab.utilities.helper.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class NegLab {

    @Test
    public void FReND() throws URISyntaxException, IOException {

        URI sURI = new URI("https://raw.githubusercontent.com/lattice-8094/FReND/main/my_xml_corpus.xml");

        File pFile = FileUtils.downloadFile(sURI.toString());
        pFile.deleteOnExit();

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try {

            // optional, but recommended
            // process XML securely, avoid attacks like XML External Entities (XXE)
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

            // parse XML file
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.parse(pFile);

            NodeList annotations = doc.getElementsByTagName("Document");

            for (int a = 0; a < annotations.getLength(); a++) {

                Node pDocumentNode = annotations.item(a);

                List<Node> pParts = getNodesFromXML(pDocumentNode, "DocumentPart");
                String sDocID = getSingleNodesFromXML(pDocumentNode, "DocID").getTextContent();
                System.out.println(sDocID);
                for (Node pPart : pParts) {
                    List<Node> cueList = getNodesFromXML(pPart, "Cue");

                    for (Node cNode : cueList) {
                        List<Node> sNodes = getNodesFromXML(cNode, "Scope");
                        sNodes.forEach(scope->{
                            System.out.println(cNode.getTextContent());
                        });
                    }

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
