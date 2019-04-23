package it.polimi.middleware;

import it.polimi.middleware.model.TweetFilter;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@WebSocket
public class WebSocketController {
    private static final Logger logger = Logger.getLogger(WebSocketController.class);

    private final ConsumersOrchestrator consumersOrchestrator;//dependency
    private final TweetProducer producer;//dependency

    private final Map<Session, WebSocketClient> connectedUsers = new ConcurrentHashMap<>();//Connected users

    WebSocketController(ConsumersOrchestrator consumersOrchestrator, TweetProducer producer) {
        this.consumersOrchestrator = consumersOrchestrator;
        this.producer = producer;
    }

    @OnWebSocketConnect
    public void connected(Session session) {
        logger.info("A new client has connected via WS.");
        //Create a new user object when a new connection occurs.
        connectedUsers.put(session, new WebSocketClient(consumersOrchestrator, producer, session));
    }

    @OnWebSocketClose
    public void closed(Session session, int statusCode, String reason) {
        logger.info("A WS client has just gone.");
        connectedUsers.get(session).tearDown();//housekeeping
        connectedUsers.remove(session);
    }

    @OnWebSocketMessage
    public void message(Session session, String message) throws IOException {
        //First retrieve filter and query.
        TweetFilter filter;
        try {
            filter = TweetFilter.valueOf(message.split("/")[0].toUpperCase());
        } catch (IllegalArgumentException iae) {
            logger.error("Received filter is not valid.", iae);
            return;
        }
        String query = message.split("/")[1];

        logger.info("Got: " + message + "; filter is " + filter + ", query is " + query);

        //Since the message is valid we can handle the logic.
        connectedUsers.get(session).onMessage(filter, query);
    }
}
