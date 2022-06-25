package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import io.socket.client.IO;
import io.socket.client.Socket;

import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketIO {
    /***
     * @author
     * Givara Ebo
     */

    public static Socket client;
    public static String typeSystem;

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
                System.out.println("[SocketIO]: "+counter+" Composer: with socket id: "+client.id()+" is connected with Annotator: " +objects[0]);
            });
            //socket.close();
        });
    }

    public static void wsDisConnected(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[SocketIO]: Server(Annotator) is disconnected"));
    }

    public static void wsOnMessage(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[SocketIO]: Message is arrived"));
    }
    public static void close(){
        client.close();
        System.out.println("[SocketIO]: Client(Composer) ist disconnected");
    }


    public SocketIO(String websocketLink){
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
