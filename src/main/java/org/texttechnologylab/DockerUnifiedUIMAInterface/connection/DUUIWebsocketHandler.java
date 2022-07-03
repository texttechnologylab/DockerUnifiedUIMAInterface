package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class DUUIWebsocketHandler implements IDUUIConnectionHandler {
    /***
     * @author
     * Dawit Terefer
     */
    private boolean success;
    private String uri;
    private WebsocketClient client;
    public DUUIWebsocketHandler() {
    }


    @Override
    public void initiate(String uri) throws URISyntaxException {
        this.uri = uri.replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET;
        this.client = new WebsocketClient(new URI(this.uri));
        /***
         * @edited
         * Givara Ebo
         */

        try {
            this.client.connectBlocking();
        } catch (InterruptedException e) {
            System.out.println("[DUUIWebsocketHandler] Connection to websocket failed!");
            System.exit(1);
        }
        //this.socketIO.close();


    }

    @Override
    public boolean success() {
        return success;
    }

    @Override
    public byte[] sendAwaitResponse(byte[] serializedObject) throws IOException, InterruptedException {
        try {
            client.send(serializedObject);
            System.out.println("[DUUIWebsocketHandler]: has been sent"+
                    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(serializedObject)));
        } catch (WebsocketNotConnectedException e) {
            System.out.println("[DUUIWebsocketHandler]: WebsocketNotConnectedException Error not working");
        }
        System.out.println("[DUUIWebsocketHandler]: Client MessageStack "+client.messageStack);
        /***
         * @edited
         * Givara Ebo
         * hier muss auf die Antwort gewartet werden
         */

        while (client.messageStack.isEmpty()) {
            Thread.sleep(1);
            System.out.println("[DUUIWebsocketHandler]: Waiting for adding Websocket Response to MessageStack.....");
        }
        System.out.println("[DUUIWebsocketHandler]: Client MessageStack "+client.messageStack);

        byte[] result = client.messageStack.get(0);
        success = true;
        // client.close();

        return result;
    }



}
