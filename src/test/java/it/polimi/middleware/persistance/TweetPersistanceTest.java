package it.polimi.middleware.persistance;

import it.polimi.middleware.model.TweetValue;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TweetPersistanceTest {
    private TweetPersistance tweetPersistance;

    @Before
    public void setUp() throws Exception {
        if (!(new File("/tmp/testfile.txt")).delete()) {
            System.out.println("Unable to delete test file");
        }

        tweetPersistance = new TweetPersistance("testfile");
    }

    @Test
    public void writeAndRead() {
        TweetValue tweetValue1 = new TweetValue(
                "Autore",
                "Contenuto",
                System.currentTimeMillis() / 1000L,
                "Milano"
        );

        TweetValue tweetValue2 = new TweetValue(
                "Autore 2",
                "Contenuto 2",
                System.currentTimeMillis() / 1000L,
                "Parma"
        );

        tweetPersistance.write(tweetValue1, 1);
        tweetPersistance.write(tweetValue2, 2);

        List<TweetValue> tweetValues = tweetPersistance.readAll();
        assertEquals(tweetValues.size(), 2);
        assertEquals(tweetValue1, tweetValues.get(0));
        assertEquals(tweetValue2, tweetValues.get(1));
        assertEquals(2, tweetPersistance.getOffset());
        tweetPersistance.close();

    }
}