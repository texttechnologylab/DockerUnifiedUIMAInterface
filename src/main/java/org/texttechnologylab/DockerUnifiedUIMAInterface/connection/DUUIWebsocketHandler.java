package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DUUIWebsocketHandler implements IDUUIConnectionHandler {
        private boolean success;
        private String uri;
        private WebsocketClient client;

        public DUUIWebsocketHandler() {
        }


        @Override
        public void initiate(String uri) throws URISyntaxException {
            this.uri = uri.replaceFirst("http", "ws") + DUUIComposer.V1_COMPONENT_ENDPOINT_PROCESS_WEBSOCKET;
            this.client = new WebsocketClient(new URI(this.uri));
            try {
                this.client.connectBlocking();
            } catch (InterruptedException e) {
                System.out.println("[WebsocketHandler] Connection to websocket failed!");
                System.exit(1);
            }

        }

        @Override
        public boolean success() {
            return success;
        }

        @Override
        public byte[] sendAwaitResponse(byte[] serializedObject) throws IOException {

            client.send(serializedObject);
            while (client.messageStack.isEmpty()) {
                int c = 0;
            }
            byte[] result = client.messageStack.get(0);
            success = true;
            client.close();
            return result;
        }
}
