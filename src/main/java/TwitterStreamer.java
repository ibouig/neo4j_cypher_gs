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

import java.io.FileNotFoundException;
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

    private static final int BATCH = 100;

    public static void main(String... args) throws InterruptedException, MalformedURLException, URISyntaxException, FileNotFoundException, UnsupportedEncodingException {
        int maxReads = 1000000;

        //twitter
        String consumerKey="I8KdkJQ3E8YHL1zJZgz1FLJOM";
        String consumerSecret="gjzGa0XVKxcdJfmE9A5xaRyxGu1dqgJa8GoW4r1DqXMltqcbrS";
        String authToken="1635806574-9YMLOTPr5ELDE0NXpASPv3XscjUEGIFrNuZAy0Z";
        String authSecret="wP7oAOV1rcZzvlzg1t6DZvMPH9mK5Xg3iHrW2s6AuHlVM";
        //Neo4j

        String neo4j_user="neo4j";
        String neo4j_pass="159456852123";
        String neo4j_host="localhost";
        String neo4j_port="7687";
        //bolt://neo4j:****@localhost:7678          //127.0.0.1:7687
        String NEO4J_URL="bolt://"+neo4j_user+":"+neo4j_pass+"@"+neo4j_host+":"+neo4j_port;

        //Keywords
        String searchTerms = "palestine,quds,jerusalem,israel,قدس,أقصى,فلسطين,إسرائيل";


        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        List<Long> userIds = asList();
        List<String> terms = asList(searchTerms.split(","));

        String TWITTER_KEYS= consumerKey + ":" + consumerSecret + ":" + authToken + ":" + authSecret ;
        BasicClient client = configureStreamClient(msgQueue, TWITTER_KEYS, userIds, terms);

        Neo4jWriter writer = new Neo4jWriter(NEO4J_URL);
        writer.init();
        int numProcessingThreads = Math.max(1,Runtime.getRuntime().availableProcessors() - 1);
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        client.connect();

        List<String> buffer = new ArrayList<>(BATCH);
        for (int msgRead = 0; msgRead < maxReads; msgRead++) {
            if (client.isDone()) {
                System.err.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);
            if (msg == null) System.out.println("Did not receive a message in 5 seconds");
            else buffer.add(msg);

            if (buffer.size() < BATCH) continue;

            List<String> tweets = buffer;
            //Output to a file or terminal

            //System.out.println(tweets);
            //PrintWriter fileWriter = new PrintWriter("jsonFeed.json", "UTF-8");
            //fileWriter.println(tweets);
            //fileWriter.close();

            service.submit(() -> writer.insert(tweets,3));
            buffer = new ArrayList<>(BATCH);
        }


        client.stop();
        writer.close();
    }

    private static BasicClient configureStreamClient(BlockingQueue<String> msgQueue, String twitterKeys, List<Long> userIds, List<String> terms) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint()
                .followings(userIds)
                .trackTerms(terms);
        endpoint.stallWarnings(false);

        String[] keys = twitterKeys.split(":");
        Authentication auth = new OAuth1(keys[0], keys[1], keys[2], keys[3]);

        ClientBuilder builder = new ClientBuilder()
                .name("Neo4j-Twitter-Stream")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private static BasicClient configureStreamClientByLocation(BlockingQueue<String> msgQueue, String twitterKeys, List<Long> userIds, List<Location> locations) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint().locations(locations);

                //.followings(userIds)
                //.trackTerms(terms);
        endpoint.stallWarnings(false);

        String[] keys = twitterKeys.split(":");
        Authentication auth = new OAuth1(keys[0], keys[1], keys[2], keys[3]);

        ClientBuilder builder = new ClientBuilder()
                .name("Neo4j-Twitter-Stream")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private static BasicClient configureStreamClientByTrends(BlockingQueue<String> msgQueue, String twitterKeys, List<Long> userIds, List<Location> locations) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        TrendsEndpoint endpoint = new TrendsEndpoint("https://api.twitter.com/1.1/trends/place.json","GET");
        //endpoint.stallWarnings(false);

        String[] keys = twitterKeys.split(":");
        Authentication auth = new OAuth1(keys[0], keys[1], keys[2], keys[3]);

        ClientBuilder builder = new ClientBuilder()
                .name("Neo4j-Twitter-Stream")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

