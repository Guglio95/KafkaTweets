package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetFilter;
import it.polimi.middleware.model.TweetValue;
import org.apache.log4j.Logger;
import spark.Request;
import spark.Response;

import java.util.Arrays;
import java.util.stream.Collectors;

import static spark.Spark.*;

public class WebServer {
    private ConsumersOrchestrator consumersOrchestrator;
    private TweetProducer producer;
    private Gson gson = new Gson();

    private static final Logger logger = Logger.getLogger(WebServer.class);

    public WebServer(ConsumersOrchestrator consumersOrchestrator, TweetProducer producer) {
        this.consumersOrchestrator = consumersOrchestrator;
        this.producer = producer;
    }

    public void start() {
        webSocket("/tweets/ws", new WebSocketController(consumersOrchestrator, producer));
        staticFiles.location("html");

        get("", (request, response) -> {
            response.redirect("html/index.html");
            return null;
        });

        path("/tweets", () -> {
            post("", this::postNewTweet);
            get("/:isTweetPertinent/:keyword/:howmany", this::getTweetsFromTopic);
        });

        //CORS
        options("/*",
                (request, response) -> {

                    String accessControlRequestHeaders = request
                            .headers("Access-Control-Request-Headers");
                    if (accessControlRequestHeaders != null) {
                        response.header("Access-Control-Allow-Headers",
                                accessControlRequestHeaders);
                    }

                    String accessControlRequestMethod = request
                            .headers("Access-Control-Request-Method");
                    if (accessControlRequestMethod != null) {
                        response.header("Access-Control-Allow-Methods",
                                accessControlRequestMethod);
                    }

                    return "OK";
                });

        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));


    }

    private String getTweetsFromTopic(Request request, Response response) {
        String keyword = request.params(":keyword").toLowerCase();
        String stringFilter = request.params(":isTweetPertinent").toLowerCase();
        String howMany = request.params(":howmany").toLowerCase();
        TweetFilter filter;

        //Check if required fields are valid
        try {
            filter = TweetFilter.valueOf(stringFilter.toUpperCase());
        } catch (IllegalArgumentException iae) {
            response.status(422);
            return "{\"message\":\"The isTweetPertinent or quantifier specified is not supported\"}";
        }


        TweetConsumer tweetConsumer = consumersOrchestrator.getConsumer(filter);//Retireve consumer associate to this topic.
        int partitionId = CustomPartitioner.partition(producer.partitionsPerTopic(filter), keyword);
        logger.info("Client wants to read " + howMany + " tweets by " + filter + " with keyword " + keyword
                + ", we want partition " + partitionId);

        response.type("application/json");
        if (howMany.equals("all")) {
            return gson.toJson(tweetConsumer.getTopicPersistance().readAllTweets(partitionId));
        }

        return gson.toJson(
                tweetConsumer.getSlidingWindows().get(partitionId).getWindow().stream()
                        .filter(tweet -> ((TweetValue) tweet).isPertinent(filter, keyword))
                        .collect(Collectors.toList()));

    }


    private String postNewTweet(Request request, Response response) {
        //Check existence of all fields
        if (!request.queryMap("author").hasValue() ||
                !request.queryMap("content").hasValue() ||
                !request.queryMap("timestamp").hasValue() ||
                !isNumeric(request.queryMap("timestamp").value()) ||
                !request.queryMap("location").hasValue()
                ) {
            response.status(422);
            return "Missing params";
        }

        //Create new tweet
        TweetValue tweetValue = new TweetValue(
                request.queryMap().get("author").value(),
                request.queryMap().get("content").value(),
                request.queryMap().get("timestamp").integerValue(),
                request.queryMap().get("location").value()
        );

        if (request.queryMap("tags").hasValue()) {
            //Add all tags which are not empty.
            tweetValue.setTags(Arrays.asList(request.queryMap().get("tags").value().split(";"))
                    .stream().filter(item -> !item.isEmpty()).collect(Collectors.toList())
            );
        }

        if (request.queryMap("mentions").hasValue()) {
            //Add all mentions to non empty users.
            tweetValue.setMentions(Arrays.asList(request.queryMap().get("mentions").value().split(";"))
                    .stream().filter(item -> !item.isEmpty()).collect(Collectors.toList())
            );
        }

        //Enqueue new tweet
        producer.enqueue(tweetValue);
        return "OK";
    }


    private boolean isNumeric(String str) {
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c)) return false;
        }
        return true;
    }
}
