package it.polimi.middleware;

import it.polimi.middleware.model.TweetValue;

public interface TweetObserver {
    void receive(TweetValue tweetValue);
}
