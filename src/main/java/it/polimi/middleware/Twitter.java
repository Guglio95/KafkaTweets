package it.polimi.middleware;

import com.google.gson.Gson;

import static spark.Spark.*;

public class Twitter {
    private static final String TOPIC = "tweets";
    private static final TweetConsumer consumer = new TweetConsumer(TOPIC);
    private static final TweetProducer producer = new TweetProducer();

    public static void main(String [] args) {
        Gson gson = new Gson();
        staticFiles.location("/");

        path("/", () -> {
            get("", (request, response) -> {
                response.redirect("root.html");
                return null;
            });
        });
    }
}
