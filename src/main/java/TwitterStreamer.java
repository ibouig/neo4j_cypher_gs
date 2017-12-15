import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.System.getenv;
import static java.util.Arrays.asList;


public class TwitterStreamer {

    private static final int BATCH = 2;
    int maxReads = 1;

    private static Neo4jWriter writer;
    private static ExecutorService service;
    private static List<String> buffer;
    private static Twitter twitter;






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

    public static void main(String[] args) throws TwitterException, IOException, URISyntaxException, InterruptedException {
        //Neo4j
        List<String> tweets = new ArrayList<>();
        String neo4j_user="neo4j";
        String neo4j_pass="159456852123";
        String neo4j_host="localhost";
        String neo4j_port="7687";

        String NEO4J_URL="bolt://"+neo4j_user+":"+neo4j_pass+"@"+neo4j_host+":"+neo4j_port;

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        buffer = new ArrayList<>(BATCH);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);

        ConfigurationBuilder cb_ = new ConfigurationBuilder();
        cb_.setDebugEnabled(true)
                .setOAuthConsumerKey("I8KdkJQ3E8YHL1zJZgz1FLJOM")
                .setOAuthConsumerSecret("gjzGa0XVKxcdJfmE9A5xaRyxGu1dqgJa8GoW4r1DqXMltqcbrS")
                .setOAuthAccessToken("1635806574-9YMLOTPr5ELDE0NXpASPv3XscjUEGIFrNuZAy0Z")
                .setOAuthAccessTokenSecret("wP7oAOV1rcZzvlzg1t6DZvMPH9mK5Xg3iHrW2s6AuHlVM")
                .setDebugEnabled(true);



        //TwitterFactory tf = new TwitterFactory(cb.build());
        //Twitter twitter = tf.getInstance();

        writer = new Neo4jWriter(NEO4J_URL);
        writer.init();

        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                //System.out.println(copyToJsonString(status));
                //System.out.println(TwitterObjectFactory.getRawJSON( status ));
                buffer.add(copyToJsonString(status));
                //get tweets timeline
                //System.out.println(status.getUser().getScreenName());
                try {
                    List<Status> statusList = TwitterStreamer.twitter.getUserTimeline(status.getUser().getScreenName());
                    if(status.getInReplyToScreenName()!=null)
                        statusList.addAll(TwitterStreamer.twitter.getUserTimeline(status.getInReplyToScreenName()));
                    if(status.getUserMentionEntities().length!=0){
                        for (UserMentionEntity u:status.getUserMentionEntities()) {
                            statusList.addAll(TwitterStreamer.twitter.getUserTimeline(u.getScreenName()));
                        }
                    }

                    for (Status s:statusList) {
                        buffer.add(copyToJsonString(s));
                    }



                } catch (TwitterException e) {
                    e.printStackTrace();
                }


                //get tweets timeline of user whome we're replaying to
                status.getInReplyToScreenName();
                //get tweets timeline of user we mentioned in our tweet
                //status.getUserMentionEntities()[0].getScreenName();




                if (buffer.size() >= BATCH) {
                    List<String> tweets = buffer;
                    buffer = new ArrayList<>(BATCH);
                    System.out.println(tweets);

                    service.submit(() -> writer.insert(tweets,3));

                }
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {

            }

            @Override
            public void onStallWarning(StallWarning warning) {

            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        Configuration configuration = cb.build();
        TwitterStreamFactory twitterStreamFactory = new TwitterStreamFactory(configuration);
        TwitterStream twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);

        TwitterFactory tf = new TwitterFactory(cb_.build());
        twitter = tf.getInstance();
        //System.out.println(twitter.getUserTimeline());


        int numProcessingThreads = Math.max(1,Runtime.getRuntime().availableProcessors() - 1);
        service = Executors.newFixedThreadPool(numProcessingThreads);


        String msg = msgQueue.poll(5, TimeUnit.SECONDS);




        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        //twitterStream.sample();
        String[] s = {"palestine","trump"};
        twitterStream.filter(new FilterQuery().track(s));




        //Output to a file or terminal

        //System.out.println(tweets);
        //PrintWriter fileWriter = new PrintWriter("jsonFeed.json", "UTF-8");
        //fileWriter.println(tweets);
        //fileWriter.close();

        //service.submit(() -> writer.insert(tweets,3));

        ///writer.close();
    }
}

