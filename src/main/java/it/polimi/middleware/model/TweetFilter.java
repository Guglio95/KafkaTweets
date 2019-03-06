package it.polimi.middleware.model;

public enum TweetFilter {
    TAG,
    MENTION,
    LOCATION;


    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
