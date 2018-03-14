package org.elasticsearch.module.graphite.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.SpecialPermission;

public class GraphiteMockServer extends Thread {

    private int port;
    private Collection<String> content = Collections.synchronizedCollection(new ArrayList<String>());
    private ServerSocket server;
    private boolean isClosed = false;

    public GraphiteMockServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        
        try {
            server = new ServerSocket(port);
            Socket client;

            while (!isClosed) {
                if (server.isClosed()) return;

                
                client = AccessController.doPrivileged(new PrivilegedAction<Socket>() {
                    public Socket run() {
                        try {
                            return server.accept();
                        } catch (IOException e) {                            
                            return null;
                        }
                    }
                });
                if(client != null) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

                    String msg;
                    synchronized (content) {
                        while ((msg = in.readLine()) != null) {
                            content.add(msg.trim());
                        }
                    }
                }
            }

        } catch (IOException e) {
            if (!(e instanceof SocketException)) {
                e.printStackTrace();
            }
        }
    }
    
    public Collection<String> getContent(){
        synchronized(content) {
            return new ArrayList<>(content);
        }
    }

    public void close() throws Exception {
        isClosed = true;
        server.close();
    }
}
