package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);


    private boolean keepRunning = true;
    private int updateInterval;
    private long filePointer = 0;
    private final File file;
    private final EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);

                // file의 크기를 계산
                long len = this.file.length();

                if (len < this.filePointer) {
                    logger.info("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if (len > this.filePointer) {
                    readAppendAndSend();
                }
            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line = null;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }

        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(",");
        String key = tokens[0];
        String value = line.substring(key.length() + 1);

        MessageEvent messageEvent = new MessageEvent(key, value);
        this.eventHandler.onMessage(messageEvent);
    }
}
