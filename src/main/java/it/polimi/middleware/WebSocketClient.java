package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetFilter;
import it.polimi.middleware.model.TweetValue;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.Collections;

public class WebSocketClient implements TweetObserver {
    private static final Logger logger = Logger.getLogger(WebSocketClient.class);
    private Gson gson = new Gson();

    private final ConsumersOrchestrator consumersOrchestrator;
    private final Session session;

    private TweetConsumer tweetConsumer;//Tweet consumer which is serving us.

    public WebSocketClient(ConsumersOrchestrator consumersOrchestrator, Session session) {
        this.consumersOrchestrator = consumersOrchestrator;
        this.session = session;
    }

    public void tearDown() {
        //If we have registered to a consumer, de-register from it.
        if (tweetConsumer != null) tweetConsumer.removeObserver(this);
    }

    /**
     * Handles new messages from the connected client.
     *
     * @param filter
     * @param query
     * @throws IOException
     */
    public void onMessage(TweetFilter filter, String query) throws IOException {
        //If we are already registered to a consumer, de-register from it.
        if (tweetConsumer != null) tweetConsumer.removeObserver(this);

        //Get a reference to the consumer which is listening on kafka.
        tweetConsumer = consumersOrchestrator.getConsumer(filter, query);

        //Get and push sliding window to client.
        session.getRemote().sendString(gson.toJson(tweetConsumer.getSlidingWindow().getWindow()));

        //Register as an observer to that consumer.
        tweetConsumer.addObserver(this);
    }

    /**
     * Callback called from TweetConsumer when a new tweet has arrived.
     *
     * @param tweetValue
     */
    @Override
    public void receive(TweetValue tweetValue) {
        logger.info("Received a new tweet from TweetConsumer, let's send it to our client.");
        try {
            session.getRemote().sendString(gson.toJson(Collections.singleton(tweetValue)));
        } catch (IOException e) {
            logger.error("Error while sending new tweet to client", e);
        }
    }
}
