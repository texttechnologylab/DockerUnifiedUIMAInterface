package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Interface for WebSocket connections
 *
 * @author David Terefe
 */
public class WebsocketClient extends WebSocketClient{

    List<byte []> messageStack = new ArrayList<>();

    public WebsocketClient(URI serverUri) {
        super(serverUri);
    }

    /**
     * @author Dawit Terefe
     * Checks if the analysis is finished
     * by reading the last message in the message stack.
     * If the message is "200" the analysis is finished.
     *
     * @return true if analysis is finished, false otherwise.
     */
    public boolean isFinished() {
        {
            if (messageStack.size() == 0)
                return false;

            byte[] lastMessage = messageStack.get(messageStack.size() - 1);
            String last = new String(lastMessage, StandardCharsets.UTF_8);

            return last.equals("200");
        }
    }

    /**
     * @author Dawit Terefe
     * Merges JSON results from the spacy annotator.
     * Is called through the LUA-Skript in the communication layer.
     *
     * @param results List of results where the ByteArrayInputStreams are JSON files.
     * @return One ByteArrayInputstream containing all the data of the results variable.
     */
    public static ByteArrayInputStream mergeResults(List<ByteArrayInputStream> results) {

        List<Map<String, Object>> resultsMaps = results.stream()
                .map(res -> {
                    String jsonString = new String(res.readAllBytes(), StandardCharsets.UTF_8);
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> map = null;
                    try {
                        map = mapper.readValue(jsonString, Map.class);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return map != null ? map.entrySet().stream()
                            .filter(e -> e.getValue() != null)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) : null;
                }).collect(Collectors.toList());

        Map<String, Object> resultsMap = resultsMaps.stream()
                .flatMap(jsonMap -> jsonMap.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (value1, value2) -> {
                            if (value1 instanceof Iterable) {
                                List<Object> v3 = Stream.concat(((List<Object>) value1).stream(), ((List<Object>) value2).stream())
                                        .collect(Collectors.toList());
                                return v3;
                            }
                            else return value2;
                        }
                ));

        ObjectMapper mapper = new ObjectMapper();
        String jsonMap;
        try {
            jsonMap = mapper.writeValueAsString(resultsMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            jsonMap = "";
        }

        byte[] jsonBytes = jsonMap.getBytes(StandardCharsets.UTF_8);
        System.out.println("[WebsocketClient] Merged files: \n"+resultsMap);

        return new ByteArrayInputStream(jsonBytes);
    }

    /***
     * @author Givara Ebo (edited)
     */
    @Override
    public void onMessage(ByteBuffer b) {

        byte[] data = b.array();
//        System.out.println("[WebsocketClient]: ByteBuffer received: " + b);
        this.messageStack.add(data);
    }

    @Override
    public void onMessage(String s) {
//        System.out.println("[WebsocketClient]: Message Received: " + s);
        System.out.println("[WebsocketClient]: Message Received: ");
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        System.out.println("[WebsocketClient]: Opened websocket connection...");
    }

    @Override
    public void onClose(int i, String s, boolean b) {
        System.out.println("[WebsocketClient]: CLOSED: i="+i+", s="+s+", b="+b );
    }

    @Override
    public void onError(Exception e) {
        System.err.println("[WebsocketClient]: ERROR:" + e.getMessage());
        e.getStackTrace();
    }
}