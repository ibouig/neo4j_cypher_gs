import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;


/**
 * @author saidbouig
 */
class Neo4jWriter {
    static String STATEMENT = "UNWIND {tweets} AS t\n" +
            "   WITH t\n" +
            "       \n" +
            "   \n" +
            "   WHERE t.id is not null\n" +
            "   MERGE (tweet:Tweet {id:t.id})\n" +
            "   SET\n" +
            "       tweet.text = t.text,\n" +
            "       tweet.createdAt = t.createdAt,\n" +
            "       tweet.source = t.source"
            ;
    private Driver driver;

    public Neo4jWriter(String neo4jUrl) throws URISyntaxException {
        URI boltUri = new URI(neo4jUrl);
        String[] authInfo = boltUri.getUserInfo().split(":");
        driver = GraphDatabase.driver(boltUri, AuthTokens.basic(authInfo[0], authInfo[1]));
    }

    public void init() {
        try (Session session = driver.session()) {
            session.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (t:Tag) ASSERT t.name IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (p:Place) ASSERT p.id IS UNIQUE");
        }
    }

    public void close() {
        driver.close();
    }

    public int insert(List<String> tweets, int retries) {
        //System.out.println("TWEET "+tweets.get(0) );
        while (retries > 0) {
            try (Session session = driver.session()) {
                Gson gson = new GsonBuilder().create()
                        ;



                List<Map> statuses = tweets.stream().map((s) -> gson.fromJson(s, Map.class)).collect(toList());
                long time = System.nanoTime();

                ResultSummary result = session.run(STATEMENT, Values.parameters("tweets", statuses)).consume();
                int created = result.counters().nodesCreated();
                //System.out.println("STATUS "+statuses.get(0));


                System.out.println(created+" nodes in "+ TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-time)+" ms");
                System.out.flush();

                //System.out.println("STATUS "+statuses.get(0)+"," );

                if(created == 0 ) {
                    System.out.println(" ############################## " +statuses.get(0).get("id"));



                }

                //System.out.println(tweets.get(0));
                //System.out.println(tweets.get(1));

                //System.out.println(statuses.get(0));
                //System.out.println(statuses.get(1));

                return created;
            } catch (Exception e) {
                System.err.println(e.getClass().getSimpleName() + ":" + e.getMessage()+" retries left "+retries);
                retries--;
            }
        }
        return -1;
    }



}
