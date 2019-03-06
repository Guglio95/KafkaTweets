package it.polimi.middleware.persistance;

import com.google.gson.Gson;
import it.polimi.middleware.model.TweetValue;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TweetPersistance {

    private String dbFile;
    private BufferedWriter writer;
    private Gson gson = new Gson();

    private static final Logger logger = Logger.getLogger(TweetPersistance.class);

    public TweetPersistance(String dbName) {
        dbFile = "/tmp/" + dbName + ".txt";
        try {
            writer = new BufferedWriter(new FileWriter(dbFile, true));
        } catch (IOException ioe) {
            logger.error("IOException while opening file " + dbName, ioe);
        }
    }

    public void write(TweetValue tweetValue, long offset) {
        try {
            writer.append(gson.toJson(new TweetValueWithOffset(tweetValue, offset)) + "\n");
            writer.flush();
            logger.info("Appending tweet found at offset " + offset + " to filename " + dbFile);
        } catch (IOException e) {
            logger.error("Error while writing to file " + dbFile, e);
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            logger.error("Unable to tearDown " + dbFile, e);
        }
    }

    public List<TweetValue> readAll() {
        try (BufferedReader reader = new BufferedReader(new FileReader(dbFile))) {

            Gson gson = new Gson();
            logger.info("Reading all stored tweets from " + dbFile);
            return reader.lines()
                    .map(line -> gson.fromJson(line, TweetValueWithOffset.class)
                            .getTweetValue())
                    .collect(Collectors.toList());

        } catch (IOException e) {
            logger.error("IOException reading tweets from " + dbFile, e);
            return Collections.emptyList();
        }
    }

    public long getOffset() {
        try (BufferedReader reader = new BufferedReader(new FileReader(dbFile))) {

            String last = null, line;

            while ((line = reader.readLine()) != null) {
                last = line;
            }

            if (last == null) {
                logger.info("File " + dbFile + " does not contain an offset because it's empty.");
                return 0L;
            }
            return gson.fromJson(last, TweetValueWithOffset.class).getOffset();
        } catch (IOException e) {
            logger.error("Error reading offset line from " + dbFile, e);
            return 0L;
        }
    }


    private class TweetValueWithOffset {
        private TweetValue tweetValue;
        private long offset;

        TweetValueWithOffset(TweetValue tweetValue, long offset) {
            this.tweetValue = tweetValue;
            this.offset = offset;
        }

        TweetValue getTweetValue() {
            return tweetValue;
        }

        long getOffset() {
            return offset;
        }
    }
}
