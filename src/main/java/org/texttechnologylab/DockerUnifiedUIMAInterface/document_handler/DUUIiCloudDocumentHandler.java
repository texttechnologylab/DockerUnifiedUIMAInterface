package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import com.github.tmyroadctfig.icloud4j.DriveNode;
import com.github.tmyroadctfig.icloud4j.DriveService;
import com.github.tmyroadctfig.icloud4j.ICloudService;
import com.github.tmyroadctfig.icloud4j.json.TrustedDevice;
import org.apache.http.StatusLine;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

public class DUUIiCloudDocumentHandler implements IDUUIDocumentHandler {

    ICloudService iCloudService;
    private String username;
    private char[] password;

    public DUUIiCloudDocumentHandler(String username, String password) {
        this.username = username;
        this.password = password.toCharArray();

        iCloudService = new ICloudService("");
        StatusLine statusLine =  iCloudService.getIdmsaService().authenticateViaIdmsa(username, this.password);

        System.out.println("Code eingeben:");
        Scanner scanner = new Scanner(System.in);
        String code = scanner.nextLine();

        iCloudService.getIdmsaService().validateAutomaticVerificationCode(code);

        DriveService driveService = new DriveService(iCloudService);


        // Act
        DriveNode root = driveService.getRoot();

        System.out.println(root.getNodeDetails());
        System.out.println(root.getType());

        List<DriveNode> children = root.getChildren();
//        System.out.println(String.join("\n", children));
        children.forEach(System.out::println);

    }

    public static void main(String[] args) {
        DUUIiCloudDocumentHandler handler  = new DUUIiCloudDocumentHandler(
                "dawitterefe@icloud.com",
                "Dttnetwork2001!");

        System.out.println(handler.iCloudService.getLoginInfo());
    }

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {

    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) throws IOException {

    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        return null;
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        return List.of();
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {
        return List.of();
    }
}
