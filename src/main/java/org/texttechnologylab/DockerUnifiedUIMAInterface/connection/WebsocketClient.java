package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class WebsocketClient extends WebSocketClient implements WebsocketClientInterface{
    @Override
    public void onOpen() {

    }

    public interface SocketListener  {
        void onOpen(ServerHandshake serverHandshake);
        void onMessage(String s);
        void onClose(int i, String s, boolean b);

    }
    SocketListener listener;
    List<byte []> messageStack = new LinkedList<>();

    public WebsocketClient(URI serverUri) {
        super(serverUri);
    }

    /***
     * @edited
     * Givara Ebo
     */

    @Override
    public void onMessage(ByteBuffer b) {


        byte[] data = b.array();
        System.out.println("[WebsocketClient]: ByteBuffer RECEIVED: " + b);
        String jsonString = StandardCharsets.UTF_8.decode(b).toString();
        System.out.println("[WebsocketClient]: ByteBuffer to String "+jsonString);
        this.messageStack.add(data);
    }

    @Override
    public void onMessage(String s) {
        System.out.println("[WebsocketClient]: String RECEIVED: " + s);

        listener.onMessage(s);
    }
    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("[WebsocketClient]: Open websocket connection...");
        //listener.onOpen(serverHandshake);

    }
    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("[WebsocketClient]: CLOSED: i="+i+", s="+s+", b="+b);
        listener.onClose(i, s, b);
    }

    @Override
    public void onError(Exception e) {
    }
}