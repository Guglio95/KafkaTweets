package it.polimi.middleware.model;

import java.util.ArrayList;
import java.util.List;

public class TweetDAO {
    private List<TweetValue> tweetValues;
    private long offset;

    public TweetDAO() {
        this.tweetValues = new ArrayList<>();
        this.offset = 0;
    }

    public boolean persists(List<TweetValue> tweetValues, long offset) {
        this.tweetValues.addAll(tweetValues);
        this.offset = offset;
        return true;
    }

    public List<TweetValue> getAll() {
        return new ArrayList<>(tweetValues);
    }

    public long getOffset() {
        return offset;
    }
}
