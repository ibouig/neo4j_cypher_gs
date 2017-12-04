import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.System.getenv;
import static java.util.Arrays.asList;


public class TwitterStreamer {

    private static final int BATCH = 100;

    public static void main(String... args) throws InterruptedException, MalformedURLException, URISyntaxException {
        int maxReads = 1000000;

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        List<Long> userIds = asList();
        String searchTerms = getenv("TWITTER_TERMS") != null ? getenv("TWITTER_TERMS") : "happy";
        List<String> terms = asList(searchTerms.split(","));
        //BasicClient client = configureStreamClient(msgQueue, getenv("TWITTER_KEYS"), userIds, terms);
        //TWITTER_KEYS="consumerKey:consumerSecret:authToken:authSecret";

        String TWITTER_KEYS="I8KdkJQ3E8YHL1zJZgz1FLJOM:gjzGa0XVKxcdJfmE9A5xaRyxGu1dqgJa8GoW4r1DqXMltqcbrS:1635806574-9YMLOTPr5ELDE0NXpASPv3XscjUEGIFrNuZAy0Z:wP7oAOV1rcZzvlzg1t6DZvMPH9mK5Xg3iHrW2s6AuHlVM";
        BasicClient client = configureStreamClient(msgQueue, TWITTER_KEYS, userIds, terms);
        //TwitterNeo4jWriter writer = new TwitterNeo4jWriter(getenv("NEO4J_URL"));
        String NEO4J_URL = "bolt://neo4j:159456852123@localhost:7687";//bolt://neo4j:****@localhost:7678
        //127.0.0.1:7687
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
}

