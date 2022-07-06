package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import io.socket.client.IO;
import io.socket.client.Socket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class DUUIWebsocketHandler  {
    /***
     * @author
     * Dawit Terefer
     */
    /***
     * @author
     * Givara Ebo
     */

    public static Socket client;

    static {
    }
    public static String getId(){
        return client.id();
    }

    public static void wsConnected(){
        client.on(Socket.EVENT_CONNECT, args -> {
            AtomicInteger counter = new AtomicInteger();
            client.emit("please_send_server_id", "please send server id");
            client.on("sever_id", objects ->{
                counter.addAndGet(1);
                System.out.println("[DUUIWebsocketHandler]: "+counter+" Composer: with socket id: "+client.id()+" is connected with Annotator: " +objects[0]);
            });
            //socket.close();
        });
    }

    public static void wsOnMessage(){
        /*
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[DUUIWebsocketHandler]: Message is arrived"));

         */
    }
    public static void wsDisConnected(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[DUUIWebsocketHandler]: Server(Spacy Annotator) is disconnected"));

    }




    public DUUIWebsocketHandler(String websocketLink){
        try {
            client = IO.socket(websocketLink);

        }catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        wsConnected();
        wsDisConnected();
        wsOnMessage();
        // open connection
        client.open();
    }



}
