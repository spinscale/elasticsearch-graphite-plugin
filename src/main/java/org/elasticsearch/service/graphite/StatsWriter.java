package org.elasticsearch.service.graphite;

import java.io.IOException;

public interface StatsWriter{
    
    void open() throws IOException;
    void write(char c) throws IOException;
    void write(String message) throws IOException;
    void flush() throws IOException;
    void flushAndClose();
}
