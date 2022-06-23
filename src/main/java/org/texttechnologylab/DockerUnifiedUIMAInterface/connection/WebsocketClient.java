package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class WebsocketClient extends WebSocketClient {

    public List<byte []> messageStack = new LinkedList<>();

    public WebsocketClient(URI serverUri) {
        super(serverUri);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
    }

    @Override
    public void onMessage(ByteBuffer b) {
        byte[] data = b.array();
        this.messageStack.add(data);
    }

    @Override
    public void onMessage(String s) {
        System.out.println("RECEIVED: " + s);
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("CLOSED: i="+i+", s="+s+", b="+b);
    }

    @Override
    public void onError(Exception e) {
    }
}