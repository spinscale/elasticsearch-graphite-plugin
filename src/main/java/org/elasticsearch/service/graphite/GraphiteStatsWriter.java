package org.elasticsearch.service.graphite;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import javax.xml.bind.Marshaller.Listener;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.logging.ESLoggerFactory;

public class GraphiteStatsWriter implements StatsWriter{
    
    private final Logger logger = ESLoggerFactory.getLogger(getClass().getName());
    private Socket socket = null;
    private BufferedWriter writer;
    private String host;
    private int port = 2003;
    
    public GraphiteStatsWriter(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void open() throws IOException {
        SpecialPermission.check();
        socket = AccessController.doPrivileged(new PrivilegedAction<Socket>() {
            @Override
            public Socket run() {
                try {
                    return new Socket(host, port);                    
                }catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }            
        });
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }
    
    @Override
    public void write(char c) throws IOException {
        writer.write(c);
        
    }

    @Override
    public void write(String message) throws IOException {
        writer.write(message);
        
    }

    @Override
    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }
    
    @Override
    public void flushAndClose() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                logger.info("Error while flushing writer:", e);
            }
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.info("Error while socket:", e);
            }
        }        
    }
}



