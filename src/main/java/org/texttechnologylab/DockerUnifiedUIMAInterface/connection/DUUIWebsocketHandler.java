package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import io.socket.client.Ack;
import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;
import org.texttechnologylab.duui.ReproducibleAnnotation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DUUIWebsocketHandler implements IDUUIConnectionHandler{
    /***
     * @author
     * Dawit Terefe
     */
    /***
     * @author
     * Givara Ebo
     */
    private byte[] send;
    private byte[] receive;

    public Socket client;
    private static List<Socket> clients = new ArrayList<>();

    static {
    }
    public String getId(){
        return client.id();
    }

    public void wsConnected(){
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

    public void wsOnMessage(){
        /*
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[DUUIWebsocketHandler]: Message is arrived"));
         */
    }

    public byte[] setSend(byte[] msg){
        return this.send = msg;
    }
    public byte[] getReceive(){


        return this.receive;
    }

    public void wsDisConnected(){
        client.on(Socket.EVENT_DISCONNECT, objects ->
                System.out.println("[DUUIWebsocketHandler]: Server(Spacy Annotator) is disconnected"));

    }

    public Socket getClient () {
        return this.client;
    }

    public static List<Socket> getClients(){
        return clients;
    }

    public byte[] get(byte[] jc) {

        final ByteArrayInputStream[] st = {null};

        client.emit("json", jc, (Ack) objects -> {

            System.out.println("[DUUIWebsocketHandler]: Message received "+
                    StandardCharsets.UTF_8.decode(ByteBuffer.wrap((byte[]) objects[0])));

            byte[] sioresult = (byte[]) objects[0];

            st[0] = new ByteArrayInputStream(sioresult);

        });

        return st[0].readAllBytes();
    }

    public void close() {
        client.close();
    }

    public DUUIWebsocketHandler(String websocketLink){
        try {
            this.client = IO.socket(websocketLink);

        }catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        wsConnected();
        wsDisConnected();
        wsOnMessage();
        // open connection
        this.client.open();
        DUUIComposer._clients.add(this);
    }



}