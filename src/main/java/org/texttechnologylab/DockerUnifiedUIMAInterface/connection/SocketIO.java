package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;

import java.net.URISyntaxException;

public class SocketIO {

    public static Socket client;

    static {
    }


    public static void wsConnected(){
        client.on(Socket.EVENT_CONNECT, (Emitter.Listener) args -> {
            client.emit("1: Client "+client.id()+" is connected");
            //socket.close();
        });
    }

    public static void wsDisConnected(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("Server is disconnected"));
    }

    public static void wsOnMessage(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("Server is disconnected"));
    }
    public static void close(){
        client.close();
        System.out.println("WebSocket's client ist closed");
    }


    public SocketIO(String websocketLink){

        try {
            client = IO.socket(websocketLink);
        } catch (URISyntaxException e) {
            System.out.println("Websocket ist not connected");
        }
        wsConnected();
        client.open();
    }



}
