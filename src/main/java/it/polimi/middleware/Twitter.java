package it.polimi.middleware;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Twitter {

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.INFO);

        //Create dependencies
        final TweetProducer tweetsProducer = new TweetProducer();//A single producer can write to any queue
        ConsumersOrchestrator consumersOrchestrator = new ConsumersOrchestrator();//A consumer can only listen to a queue.
        WebServer webServer = new WebServer(consumersOrchestrator, tweetsProducer);

        //Start webserver
        webServer.start();
    }
}
