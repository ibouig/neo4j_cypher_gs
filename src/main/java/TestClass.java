import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;

public class TestClass {


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


    public static void main2(String[] args) throws TwitterException {


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);


        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        Query query = new Query("saidbouig");
        QueryResult result = twitter.search(query);

        List<String> tweets = new ArrayList<>();

        for (Status status:result.getTweets()) {
            tweets.add(copyToJsonString(status));
        }

        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }
        System.out.println(tweets);

    }

    public static void main1(String[] args) throws TwitterException {


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);


        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        Query query = new Query("saidbouig");
        //QueryResult result = twitter.search(query);

        List<String> tweets = new ArrayList<>();

        for (Status status:twitter.getUserTimeline("mkbhd")) {
            tweets.add(copyToJsonString(status));
        }
        /*
        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }
        */
        System.out.println(tweets);

    }
    public static void main(String[] args) throws TwitterException {


        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("JqIPBeczDxrO1bXlNnQr36wh7")
                .setOAuthConsumerSecret("V7SkD107JcsPwoA7eeCYXpN78Bq5ySJ4xriVs85pZ6yASxSnOK")
                .setOAuthAccessToken("2736990925-jbottSBXTRZYEkttWEeT9sOfgpmfQDDQBcOX8xP")
                .setOAuthAccessTokenSecret("qiVi76t9P82CvA7gboJ1r17Mech3kbGrdan0YQenLVE5q")
                .setDebugEnabled(true);


        Twitter twitter = new TwitterFactory(cb.build()).getInstance();

        Query query = new Query("saidbouig");
        //QueryResult result = twitter.search(query);

        List<String> tweets = new ArrayList<>();

        Status status = twitter.showStatus(Long.parseLong("941663799232385024"));
            tweets.add(copyToJsonString(status));

        /*
        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }
        */
        System.out.println(tweets);

    }

}
