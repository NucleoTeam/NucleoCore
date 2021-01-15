package com.synload.nucleo.interlink;

public interface InterlinkServer extends Runnable {
    //InterlinkServer(int port, InterlinkHandler interlinkHandler);
    InterlinkHandler getInterlinkHandler();
    void close();
}
