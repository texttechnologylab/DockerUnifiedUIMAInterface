package org.texttechnologylab.DockerUnifiedUIMAInterface.websocket;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

public class WebsocketClient extends WebSocketClient {

    public List<byte []> messageStack = new LinkedList<>();

    public static void main(String[] args) {

        // Echo Test
        String URI = "ws://localhost:7890";
        echoServer("ws://localhost:7890/ws");

    }

    public WebsocketClient(URI serverUri) {
        super(serverUri);
    }

    public byte[] sendAndAwaitResponse(byte[] jc) {

        send(jc);

        while (this.messageStack.isEmpty()) {
            int c = 0;
        }

        this.close();
        return this.messageStack.get(0);
    }

    public static void echoServer(String uri) {
        try {
            WebsocketClient c = new WebsocketClient(new URI(uri));
            System.out.println("CONNECTION ESTABLISHED: " + c.connectBlocking());
            byte[] message = "SOMETHING".getBytes(StandardCharsets.UTF_8);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            output.write(101);
            output.write(message);
            c.send(output.toByteArray());

            ByteArrayOutputStream output2 = new ByteArrayOutputStream();
            output2.write(107);
            output2.write(message);
            c.send(output2.toByteArray());

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
