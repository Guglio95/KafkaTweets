package it.polimi.middleware;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Twitter {

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.INFO);

        String kafka1_URL = System.getenv("KAFKA1_URL");
        if (kafka1_URL == null){
            Logger.getRootLogger().fatal("Missing env 'KAFKA1_URL'");
            System.exit(1);
        }

        //Create dependencies
        final TweetProducer tweetsProducer = new TweetProducer(kafka1_URL);//A single producer can write to any queue
        ConsumersOrchestrator consumersOrchestrator = new ConsumersOrchestrator(kafka1_URL);//A consumer can only listen to a queue.
        WebServer webServer = new WebServer(consumersOrchestrator, tweetsProducer);

        //Start webserver
        webServer.start();
    }
}
