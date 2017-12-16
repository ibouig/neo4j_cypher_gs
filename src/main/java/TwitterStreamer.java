import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author saidbouig
 */
public class TwitterStreamer {

    private static final int BATCH = 2;// Controls buffer's size
    private static Neo4jWriter writer;
    private static ExecutorService service;
    private static List<String> buffer;
    private static Twitter twitter;
    private static String neo4j_user="neo4j";
    private static String neo4j_pass="159456852123";
    private static String neo4j_host="localhost";
    private static String neo4j_port="7687";
    private static String[] twitterTerms = {"palestine","trump"};

    public static void main(String[] args) throws TwitterException, IOException, URISyntaxException, InterruptedException {
        //Neo4j
        String NEO4J_URL="bolt://"+neo4j_user+":"+neo4j_pass+"@"+neo4j_host+":"+neo4j_port;

        List<String> tweets = new ArrayList<>();
        buffer = new ArrayList<>(BATCH);

        //Twitter Stream config
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);

        //Twitter api config
        ConfigurationBuilder cb_ = new ConfigurationBuilder();
        cb_.setDebugEnabled(true)
                .setOAuthConsumerKey("I8KdkJQ3E8YHL1zJZgz1FLJOM")
                .setOAuthConsumerSecret("gjzGa0XVKxcdJfmE9A5xaRyxGu1dqgJa8GoW4r1DqXMltqcbrS")
                .setOAuthAccessToken("1635806574-9YMLOTPr5ELDE0NXpASPv3XscjUEGIFrNuZAy0Z")
                .setOAuthAccessTokenSecret("wP7oAOV1rcZzvlzg1t6DZvMPH9mK5Xg3iHrW2s6AuHlVM")
                .setDebugEnabled(true);

        //initialize Neo4jWriter
        writer = new Neo4jWriter(NEO4J_URL);
        writer.init();
        //Listener for Twitter Streamer
        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                //add current tweet
                buffer.add(copyToJsonString(status));
                //get tweets related to that tweet
                try {
                    //adding tweets from user's timeline
                    List<Status> statusList = TwitterStreamer.twitter.getUserTimeline(status.getUser().getScreenName());
                    //adding tweets from user's timeline to whome current tweet replies
                    if(status.getInReplyToScreenName()!=null)
                        statusList.addAll(TwitterStreamer.twitter.getUserTimeline(status.getInReplyToScreenName()));
                    //adding tweets from users timeline , mentioned in current tweet
                    if(status.getUserMentionEntities().length!=0){
                        for (UserMentionEntity u:status.getUserMentionEntities()) {
                            statusList.addAll(TwitterStreamer.twitter.getUserTimeline(u.getScreenName()));
                        }
                    }
                    //adding result to buffer
                    for (Status s:statusList) {
                        buffer.add(copyToJsonString(s));
                    }

                } catch (TwitterException e) {
                    e.printStackTrace();
                }
                //insert tweets to neo4j db when buffer is full
                if (buffer.size() >= BATCH) {
                    List<String> tweets = buffer;
                    buffer = new ArrayList<>(BATCH);
                    System.out.println(tweets);

                    service.submit(() -> writer.insert(tweets,3));
                }
            }
            @Override public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            @Override public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            @Override public void onScrubGeo(long userId, long upToStatusId) {}
            @Override public void onStallWarning(StallWarning warning) {}

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        //Initialize Twitter streamer
        Configuration configuration = cb.build();
        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configuration);
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);
        //Initialize Twitter API
        TwitterFactory tf = new TwitterFactory(cb_.build());
        twitter = tf.getInstance();
        //Threads
        int numProcessingThreads = Math.max(1,Runtime.getRuntime().availableProcessors() - 1);
        service = Executors.newFixedThreadPool(numProcessingThreads);

        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        //twitterStream.sample();

        twitterStream.filter(new FilterQuery().track(twitterTerms));

    }

    /**
     * Transforme A tweet Object (Status) into a Json String
     * @param modelObject
     * @return String
     */
    public static String copyToJsonString(Object modelObject ){
        String data=null;
        try {
            Gson gson = new GsonBuilder()
                    .create();
            data=gson.toJson(modelObject);
            //System.out.println(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * Export tweets into a file  fileName ="jsonFeed.json"
     * @param tweets
     */
    public static void exportTweets(List<String> tweets, String fileName){
        try {
            PrintWriter fileWriter = new PrintWriter(fileName, "UTF-8");
            fileWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }
}

