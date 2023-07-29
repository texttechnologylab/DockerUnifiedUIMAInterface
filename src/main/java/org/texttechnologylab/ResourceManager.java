package org.texttechnologylab;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Stream;

public class ResourceManager {
    
    ArrayBlockingQueue<ByteArrayOutputStream> _bytestreams;

    static final ResourceManager _rm = new ResourceManager();  

    public static ResourceManager getInstance() {
        return _rm; 
    };

    private ResourceManager() {
    }

    public ResourceManager setWorkers(int workers) {
        _bytestreams = new ArrayBlockingQueue<>(workers);
        
        Stream.generate(() -> new ByteArrayOutputStream(1024*1024))
            .limit(workers)
            .forEach(_bytestreams::add);
        
        return this; 
    }

    public ByteArrayOutputStream takeByteStream() throws InterruptedException {
        return _bytestreams.take();
    }

    public void returnByteStream(ByteArrayOutputStream bs) {
        _bytestreams.add(bs);
    }
}
