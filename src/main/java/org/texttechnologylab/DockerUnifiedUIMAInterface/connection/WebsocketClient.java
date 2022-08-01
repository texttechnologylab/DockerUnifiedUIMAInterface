package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class WebsocketClient extends WebSocketClient{


    public interface SocketListener  {
        void onOpen(ServerHandshake serverHandshake);
        void onMessage(String s);
        void onClose(int i, String s, boolean b);

    }


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
//        System.out.println("[WebsocketClient]: ByteBuffer received: " + b);

        //String jsonString = StandardCharsets.UTF_8.decode(b).toString();

        //System.out.println("[WebsocketClient]: ByteBuffer received: "+jsonString);
        this.messageStack.add(data);
    }

    @Override
    public void onMessage(String s) {
        System.out.println("[WebsocketClient]: Message Received: " + s);

    }
    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("[WebsocketClient]: Opened websocket connection...");

    }
    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("[WebsocketClient]: CLOSED: i="+i+", s="+s+", b="+b +" #####################################  Closed");
    }

    @Override
    public void onError(Exception e) {
    }
}