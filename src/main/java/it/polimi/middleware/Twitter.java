package it.polimi.middleware;

import com.google.gson.Gson;
import it.polimi.middleware.model.Resp;
import it.polimi.middleware.model.TweetValue;

import static spark.Spark.*;

public class Twitter {
    private static final String TOPIC = "tweets";
    //private static final TweetConsumer consumer = new TweetConsumer(TOPIC);
    //private static final TweetProducer producer = new TweetProducer();

    public static void main(String [] args) {
        Gson gson = new Gson();
        staticFiles.location("/html");

        path("/", () -> {
            get("", (req,res) -> {
                res.redirect("root.html");
                return null;
            });
        });

        path("/tweets", () -> {
            get("", (req, res) -> null);
            post("", (req, res) -> {
                res.type("application/json");
                TweetValue newTweet = gson.fromJson(req.body(), TweetValue.class);
                //TODO: persist
                res.status(201);
                return gson.toJson(new Resp(201));
            });
        });
    }
}
