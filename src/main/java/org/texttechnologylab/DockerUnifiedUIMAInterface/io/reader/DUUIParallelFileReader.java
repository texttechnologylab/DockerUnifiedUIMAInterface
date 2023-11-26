package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parallel File Reader - not finished!
 *
 * @author Giuseppe Abrami
 */
public class DUUIParallelFileReader implements Runnable {

    public static AtomicInteger iDebugCount = new AtomicInteger(0);
    Collection<String> pResult = null;
    File pFile = null;
    String sEnding = "";
    boolean bRunning = true;
    int iThreadLevels = 0;
    Set<DUUIParallelFileReader> subThreads = new HashSet<>(0);
    private int iMaxThreads = 3;

    public DUUIParallelFileReader(File pFile, String sEnding, Collection<String> pResult) {
        this(pFile, sEnding, pResult, 0);
    }

    public DUUIParallelFileReader(File pFile, String sEnding, Collection<String> pResult, int iThreadLevels) {
        this.pFile = pFile;
        this.sEnding = sEnding;
        this.pResult = pResult;
        this.iThreadLevels = iThreadLevels;
        init();
    }

    private void init() {

        if (pFile.isFile()) {
            if (pFile.getName().endsWith(sEnding) || sEnding.length() == 0) {
                pResult.add(pFile.toPath().toString());
            }
        } else if (iThreadLevels > 0) {
            File[] listOfFiles = pFile.listFiles();
            if (listOfFiles == null) {
                listOfFiles = new File[0];
            }
            int tempSubLevel = iThreadLevels - 1;
            for (File file : listOfFiles) {

                while (subThreads.size() == iMaxThreads) {
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    Set<DUUIParallelFileReader> removeSet = new HashSet<>(0);
                    for (DUUIParallelFileReader tp : subThreads) {
                        if (tp.isFinish()) {
                            removeSet.add(tp);
                        }
                    }
                    removeSet.forEach(t -> {
                        subThreads.remove(t);
                    });

                }

                DUUIParallelFileReader pReader = new DUUIParallelFileReader(file, sEnding, pResult, tempSubLevel);
                subThreads.add(pReader);
                Thread pThread = new Thread(pReader);
                pThread.start();

            }
        }
    }

    @Override
    public void run() {

        try {
            Files.walk(this.pFile.toPath()).filter(Files::isRegularFile).filter(p -> {
                if (sEnding.length() > 0) {
                    return p.getFileName().toString().endsWith(sEnding);
                }
                return true;
            }).forEach(path -> {
                this.pResult.add(path.toString());
                iDebugCount.incrementAndGet();
                if (iDebugCount.get() > 0 && iDebugCount.get() % 10000 == 0) {
                    System.out.println(iDebugCount.get());
                }
            });

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }


        bRunning = false;
    }

    boolean isFinish() {
        return !bRunning;
    }

}
