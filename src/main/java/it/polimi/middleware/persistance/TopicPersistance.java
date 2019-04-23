package it.polimi.middleware.persistance;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetValue;
import org.apache.log4j.Logger;

import java.util.List;

public class TopicPersistance {
    private static final Logger logger = Logger.getLogger(TopicPersistance.class);

    private String topicName;

    public TopicPersistance(String topicName) {
        this.topicName = topicName;
    }

    public Long getOffset(int partitionId) {
        FilePersistance offsetPersistance = new FilePersistance(topicName + "_offset_" + partitionId);
        return offsetPersistance.getLine();
    }

    public void storeTweet(TweetValue tweetValue, int partitionId, long offset) {
        FilePersistance tweetPersistance = new FilePersistance(topicName + "_" + partitionId);
        FilePersistance offsetPersistance = new FilePersistance(topicName + "_offset_" + partitionId);

        Gson gson = new Gson();
        tweetPersistance.appendLine(gson.toJson(tweetValue));
        offsetPersistance.storeValue(offset + "");
    }

    public List<TweetValue> readAllTweets(int partitionId) {
        FilePersistance tweetPersistance = new FilePersistance(topicName + "_" + partitionId);
        return tweetPersistance.readAll(TweetValue.class);
    }
}
