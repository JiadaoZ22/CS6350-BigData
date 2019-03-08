package ass1_2;

import org.apache.log4j.BasicConfigurator;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class GetConfiguration {
    public Twitter getNewInstance(){
        BasicConfigurator.configure();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("Gxt2FNUBRh917OhJL6y28B0ns")
                .setOAuthConsumerSecret("ajJTEIGydR9v9DTYIpa4uHMsqWNUmU7CU1fKrbL46vzE07gwPQ")
                .setOAuthAccessToken("1093588507262627840-lQy17GJ7tQJM9FRZ5jAgecrS0FzxjK")
                .setOAuthAccessTokenSecret("Cga7f4M9V2Gi7UMRYOs01Rt4wanq7SpbWB4fkZUxAE1wJ");
        TwitterFactory tf = new TwitterFactory(cb.build());
        //Twitter twitter = tf.getSingleton();
        Twitter twitter = tf.getInstance();
        return twitter;
    }
}