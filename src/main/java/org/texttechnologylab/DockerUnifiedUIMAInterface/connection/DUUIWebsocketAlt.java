package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Dawit Terefe, Givara Ebo
 *
 * Interface between DUUIComposer and WebsocketClient.
 */
@Deprecated
public class DUUIWebsocketAlt implements IDUUIConnectionHandler{

    private static List<DUUIWebsocketAlt> clients = new ArrayList<>();
    private WebsocketClient client;
    private static Map<String, WebsocketClient> _clients = new HashMap<>();

    public DUUIWebsocketAlt(String uri, int elements) throws InterruptedException, IOException {
        boolean connected;

        if (!_clients.containsKey(uri)) { // If not already connected to uri.
            this.client = new WebsocketClient(URI.create(uri+"/?tokens_num="+elements)); //  Anzahl der Tokens
            System.out.println("[DUUIWebsocketAlt]: Trying to connect to "+uri);
            connected = this.client.connectBlocking();
            _clients.put(uri, this.client);
        }
        else { // If already connected to uri.
            this.client = _clients.get(uri);
            connected = this.client.isOpen();

            if (!connected) { // If connection was closed.
                System.out.println("[DUUIWebsocketAlt]: Trying to reconnect to "+uri);
                this.client = new WebsocketClient(URI.create(uri+"/?tokens_num="+elements));
                connected = this.client.connectBlocking();
            }
        }

        if (!connected) {
            System.out.println("[DUUIWebsocketAlt] Client could not connect!");
            throw new IOException("Could not reach endpoint!");
        }

        System.out.println("[DUUIWebsocketAlt] Successfully connected to " + uri);

        this.client.setConnectionLostTimeout(0);
        DUUIComposer._clients.add(this);
        System.out.println("[DUUIWebsocketAlt] Remote URL %s is online and seems to understand DUUI V1 format!\n"+URI.create(uri));
    }

    /**
     * Sends serialized JCAS Object and returns result of analysis.
     *
     * @param jc serialized JCAS Object in bytes.
     * @return List of results.
     */
    public List<ByteArrayInputStream> send(byte[] jc) {

        this.client.send(jc);
//        System.out.println("[DUUIWebsocketAlt]: Message sending \n"+
//                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(jc)));


        while (!client.isFinished()) {
            // Stops main thread to wait for results.
            try {
                Thread.sleep(0, 1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        List<ByteArrayInputStream> results = client.messageStack.subList(0, client.messageStack.size()-1)
                .stream()
                .map(ByteArrayInputStream::new)
                .collect(Collectors.toList());
        client.messageStack = new ArrayList<>();

        System.out.println("[DUUIWebsocketAlt]: Message received "); //\n"+
//                StandardCharsets.UTF_8.decode(ByteBuffer.wrap(result)));

        return results;
    }

    /**
     * Closes connection to websocket.
     */
    public void close() {

        if (this.client.isOpen()) {
            client.close(1000, "Closed by DUUIComposer");
            while (!client.isClosed()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("[DUUIWebsocketAlt]: Handler is closed!");
        }
    }

}
