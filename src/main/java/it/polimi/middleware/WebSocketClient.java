package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetFilter;
import it.polimi.middleware.model.TweetValue;
import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WebSocketClient implements TweetObserver {
    private static final Logger logger = Logger.getLogger(WebSocketClient.class);
    private Gson gson = new Gson();

    private int clientId;

    private final ConsumersOrchestrator consumersOrchestrator;
    private final TweetProducer producer;
    private final Session session;

    private TweetConsumer tweetConsumer;//Tweet consumer which is serving us.
    private TweetFilter choosenFilter;
    private String choosenQuery;

    private static AtomicInteger globalClientId = new AtomicInteger();
    private int choosenPartitionId;

    WebSocketClient(ConsumersOrchestrator consumersOrchestrator, TweetProducer producer, Session session) {
        this.consumersOrchestrator = consumersOrchestrator;
        this.producer = producer;
        this.session = session;
        this.clientId = globalClientId.incrementAndGet();
        logger.info("Created a new WebSocketClient with ID=" + clientId);
    }

    public void tearDown() {
        //If we are registered to a consumer, de-register from it.
        if (tweetConsumer != null) tweetConsumer.removeObserver(this);
    }

    /**
     * Handles new messages from the connected client.
     *
     * @param filter
     * @param query
     * @throws IOException
     */
    void onMessage(TweetFilter filter, String query) {
        //If we are already registered to a consumer, de-register from it.
        if (tweetConsumer != null) tweetConsumer.removeObserver(this);

        //Get a reference to the consumer which is listening on kafka.
        tweetConsumer = consumersOrchestrator.getConsumer(filter);

        //Get partition in which we are interested
        int partitionId = CustomPartitioner.partition(producer.partitionsPerTopic(filter), query);


        //Update values
        this.choosenFilter = filter;
        this.choosenQuery = query;
        this.choosenPartitionId = partitionId;

        //Push sliding window to client.
        sendSlidingWindow();

        //Register as an observer to that consumer.
        tweetConsumer.addObserver(this, partitionId);
    }


    /**
     * Callback called from TweetConsumer when a new tweet has arrived on observed partition.
     **/
    @Override
    public void newTweetReceived() {
        logger.info("Received a new tweet from TweetConsumer.");

        //Send sliding window to the client.
        sendSlidingWindow();
    }

    private void sendSlidingWindow() {
        try {
            session.getRemote().sendString(
                    gson.toJson(tweetConsumer.getSlidingWindows().get(choosenPartitionId).getWindow().stream()
                            .filter(tweet -> ((TweetValue) tweet).isPertinent(choosenFilter, choosenQuery))
                            .collect(Collectors.toList())
                    )
            );
        } catch (IOException e) {
            logger.fatal("Error sending sliding window", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WebSocketClient that = (WebSocketClient) o;
        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId);
    }
}
