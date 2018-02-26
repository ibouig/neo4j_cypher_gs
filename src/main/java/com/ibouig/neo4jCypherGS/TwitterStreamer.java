package com.ibouig.neo4jCypherGS;

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

    private static final int BATCH = 10;// Controls buffer's size
    private static Neo4jWriter writer;
    private static ExecutorService service;
    private static List<String> buffer;

    private static TwitterStream twitterStream;
    private static String neo4j_user="neo4j";
    private static String neo4j_pass="159456852123";
    private static String neo4j_host="localhost";
    private static String neo4j_port="7687";
    private static String[] twitterTerms = {"palestine","quds","jerusalem","israel","إسرائيل","فلسطين","أقصى","قدس"};
    private static int accountNumber=4;
    private static int currentInUse=0;

    private static List<Twitter> twitterList;

    public static void initTwitterAccounts() {
        twitterList = new ArrayList<>();
        //webogeeks
        ConfigurationBuilder cb1 = new ConfigurationBuilder();
        cb1.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);
        //levelflow
        ConfigurationBuilder cb2 = new ConfigurationBuilder();
        cb2.setDebugEnabled(true)
                .setOAuthConsumerKey("IfKeKpi6cDiYmB3MGWyOopHTe")
                .setOAuthConsumerSecret("rqoFYjjzXfkxq5jTWEAS8nrJdYDc3frKVoYG7VMY85KDOafFx4")
                .setOAuthAccessToken("942180519609028610-STRzBLAPgkDI8OxXHvz5QmscNASmZaO")
                .setOAuthAccessTokenSecret("8D9igsVkBZ7uF2qPhTZ8bbSmvlr548yA5i908BOO5krfF")
                .setDebugEnabled(true);
        //imad
        ConfigurationBuilder cb3 = new ConfigurationBuilder();
        cb3.setDebugEnabled(true)
                .setOAuthConsumerKey("o9smIdj53ZCSuNmbGf4MT3WUy")
                .setOAuthConsumerSecret("JRvoZ5IzLJP87jGU3XAPZ4CRZ01pxQVLSZqbAY5Qjr9KSePr2F")
                .setOAuthAccessToken("3754040477-30H7jaacz17wbqBalA3jwg8lMK8aCxYLHnt4ViU")
                .setOAuthAccessTokenSecret("jPtmuZa2grTvFtnx4rSBUcV6Lkkue8bmxvygV1O4C7LoH")
                .setDebugEnabled(true);
        //sara
        ConfigurationBuilder cb4 = new ConfigurationBuilder();
        cb4.setDebugEnabled(true)
                .setOAuthConsumerKey("1DIaKVZbLG41qMfvTsU9pFPkT")
                .setOAuthConsumerSecret("AdgzNIC75VEn0vA6BFZc9rTas4HTAsABFDpqK7p6W63XTswP1l")
                .setOAuthAccessToken("942911397494706176-lNuMAcp4DdH5rgpayBP6RwbTvSnNBff")
                .setOAuthAccessTokenSecret("aw5QguG73KVTESkfmLp6BMud2VZi3UHBz71S47v9wFWGP")
                .setDebugEnabled(true);

        TwitterFactory tf = new TwitterFactory(cb1.build());
        twitterList.add(tf.getInstance());


        tf = new TwitterFactory(cb2.build());
        twitterList.add(tf.getInstance());

        tf = new TwitterFactory(cb3.build());
        twitterList.add(tf.getInstance());

        tf = new TwitterFactory(cb4.build());
        twitterList.add(tf.getInstance());


    }

    public static void main(String[] args) throws TwitterException, IOException, URISyntaxException, InterruptedException {
        //Neo4j
        String NEO4J_URL="bolt://"+neo4j_user+":"+neo4j_pass+"@"+neo4j_host+":"+neo4j_port;

        List<String> tweets = new ArrayList<>();
        buffer = new ArrayList<>(BATCH);



        //Twitter api config
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("I8KdkJQ3E8YHL1zJZgz1FLJOM")
                .setOAuthConsumerSecret("gjzGa0XVKxcdJfmE9A5xaRyxGu1dqgJa8GoW4r1DqXMltqcbrS")
                .setOAuthAccessToken("1635806574-9YMLOTPr5ELDE0NXpASPv3XscjUEGIFrNuZAy0Z")
                .setOAuthAccessTokenSecret("wP7oAOV1rcZzvlzg1t6DZvMPH9mK5Xg3iHrW2s6AuHlVM")
                .setDebugEnabled(true);

        //initialize com.ibouig.neo4jCypherGS.Neo4jWriter
        writer = new Neo4jWriter(NEO4J_URL);
        writer.init();
        //Listener for Twitter Streamer
        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                List<Status> statusList =new ArrayList<>();
                int retries = 0;
                boolean done =false;
                //get tweets related to that tweet
                do{
                    try {
                        //adding tweets from user's timeline
                        statusList.addAll(TwitterStreamer.twitterList.get(currentInUse).getUserTimeline(status.getUser().getScreenName()));
                        done=true;
                    } catch (TwitterException e) {
                        e.printStackTrace();
                        currentInUse= (currentInUse+1) % accountNumber;
                        System.out.println("1 switching twitter account to :"+currentInUse);
                        done=false;
                    }

                    retries++;

                }while(!done && retries<accountNumber);

                retries=0;
                do{
                    try {
                        //adding tweets from user's timeline to whome current tweet replies
                        if (status.getInReplyToScreenName() != null)
                            statusList.addAll(TwitterStreamer.twitterList.get(currentInUse).getUserTimeline(status.getInReplyToScreenName()));
                        done=true;

                    } catch (TwitterException e) {
                        e.printStackTrace();
                        currentInUse= (currentInUse+1) % accountNumber;
                        System.out.println("2 switching twitter account to :"+currentInUse);
                        done=false;
                    }
                    retries++;
                }while(!done && retries<accountNumber);



                //adding tweets from users timeline , mentioned in current tweet
                    if(status.getUserMentionEntities().length!=0){
                        for (UserMentionEntity u:status.getUserMentionEntities()) {
                            retries=0;
                            do{
                                try {
                                    statusList.addAll(TwitterStreamer.twitterList.get(currentInUse).getUserTimeline(u.getScreenName()));
                                    done=true;
                                } catch (TwitterException e) {
                                    e.printStackTrace();
                                    currentInUse= (currentInUse+1) % accountNumber;
                                    System.out.println("3 switching twitter account to :"+currentInUse);
                                    done=false;
                                }
                                retries++;

                            }while(!done && retries<accountNumber);

                        }
                    }

                    if(done){
                        //adding result to buffer
                        for (Status s:statusList) {
                            buffer.add(copyToJsonString(s));
                        }
                        //add current tweet
                        buffer.add(copyToJsonString(status));
                    }else{
                        System.out.println("a Tweet has been ignored du to API limite Rate");
                    }




                //insert tweets to neo4j db when buffer is full
                if (buffer.size() >= BATCH) {
                    List<String> tweets = buffer;
                    buffer = new ArrayList<>(BATCH);
                    //System.out.println(tweets);

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
        twitterStream = twitterStreamFactory.getInstance();
        twitterStream.addListener(listener);

        //Initialize Twitter API
        //TwitterFactory tf = new TwitterFactory(cb_.build());
        //twitter = tf.getInstance();

        initTwitterAccounts();
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

