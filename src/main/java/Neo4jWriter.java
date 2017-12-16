import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;


/**
 * @author saidbouig
 */
class Neo4jWriter {

    private static Driver driver;
    private static String STATEMENT = "UNWIND {tweets} AS t\n" +
            "    WITH t,\n" +
            "        t.contributorsIDs as contributors,\n" +
            "        t.retweetedStatus AS retweetedStatus,\n" +
            "        t.userMentionEntities AS userMentionEntities,\n" +
            "        t.urlEntities AS urlEntities,\n" +
            "        t.hashtagEntities AS hashtagEntities,\n" +
            "        t.mediaEntities AS mediaEntities,\n" +
            "        t.symbolEntities AS symbolEntities,\n" +
            "        t.user AS u\n" +
            "    WHERE t.id is not null\n" +
            "\n" +
            "    MERGE (tweet:Tweet {id:t.id})\n" +
            "    SET\n" +
            "        tweet.text = t.text,\n" +
            "        tweet.createdAt = t.createdAt,\n" +
            "        tweet.source = t.source,\n" +
            "        tweet.isTruncated = t.isTruncated,\n" +
            "        tweet.isFavorited = t.isFavorited,\n" +
            "        tweet.isRetweeted = t.isRetweeted,\n" +
            "        tweet.favoriteCount = t.favoriteCount,\n" +
            "        tweet.retweetCount = t.retweetCount,\n" +
            "        tweet.isPossiblySensitive = t.isPossiblySensitive,\n" +
            "        tweet.lang = t.lang,\n" +
            "        tweet.currentUserRetweetId = t.currentUserRetweetId,\n" +
            "        tweet.quotedStatusId = t.quotedStatusId\n" +
            "\n" +
            "    MERGE (user:User {screenName:u.screenName})\n" +
            "    SET\n" +
            "        user.name = u.name,\n" +
            "        user.id = u.id,\n" +
            "        user.location = u.location,\n" +
            "        user.isProtected = u.isProtected,\n" +
            "        user.followersCount = u.followersCount,\n" +
            "        user.friendsCount = u.friendsCount,\n" +
            "        user.createdAt = u.createdAt,\n" +
            "        user.favouritesCount = u.favouritesCount,\n" +
            "        user.utcOffset = u.utcOffset,\n" +
            "        user.lang = u.lang\n" +
            "\n" +
            "    MERGE (user)-[:POSTED]->(tweet)\n" +
            "    FOREACH (h IN hashtagEntities |\n" +
            "        MERGE (tag:Tag {text:LOWER(h.text)})\n" +
            "        MERGE (tag)<-[:TAGGED]-(tweet)\n" +
            "    )\n" +
            "    FOREACH (u IN [u IN urlEntities WHERE u.expandedURL IS NOT NULL] |\n" +
            "        MERGE (url:Link {url:u.expandedURL})\n" +
            "        MERGE (tweet)-[:LINKED]->(url)\n" +
            "    )\n" +
            "    FOREACH (m IN [m IN mediaEntities WHERE m.id IS NOT NULL] |\n" +
            "        MERGE (media:Media {id:m.id})\n" +
            "        ON CREATE SET\n" +
            "          media.mediaURL= m.mediaURL\n" +
            "        MERGE (tweet)-[:CONTAINED]->(media)\n" +
            "    )\n" +
            "    FOREACH (m IN userMentionEntities |\n" +
            "        MERGE (mentioned:User {screenName:m.screenName})\n" +
            "        ON CREATE SET\n" +
            "          mentioned.name = m.name,\n" +
            "          mentioned.id = m.id\n" +
            "        MERGE (tweet)-[:MENTIONED]->(mentioned)\n" +
            "    )\n" +
            "    FOREACH (r IN [r IN [t.inReplyToStatusId] WHERE r IS NOT NULL AND r <> -1 ] |\n" +
            "        MERGE (reply_tweet:Tweet {id:r})\n" +
            "        MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)\n" +
            "    )\n" +
            "    \n" +
            "\n" +
            "    FOREACH (r IN [r IN [retweetedStatus] WHERE r IS NOT NULL AND r <> -1 ] |\n" +
            "    \n" +
            "    \n" +
            "        MERGE (retweet_tweet:Tweet {id:r.id})\n" +
            "        SET\n" +
            "            retweet_tweet.text = r.text,\n" +
            "            retweet_tweet.createdAt = r.createdAt,\n" +
            "            retweet_tweet.source = r.source,\n" +
            "            retweet_tweet.isTruncated = r.isTruncated,\n" +
            "            retweet_tweet.isFavorited = r.isFavorited,\n" +
            "            retweet_tweet.isRetweeted = r.isRetweeted,\n" +
            "            retweet_tweet.favoriteCount = r.favoriteCount,\n" +
            "            retweet_tweet.retweetCount = r.retweetCount,\n" +
            "            retweet_tweet.isPossiblySensitive = r.isPossiblySensitive,\n" +
            "            retweet_tweet.lang = r.lang,\n" +
            "            retweet_tweet.currentUserRetweetId = r.currentUserRetweetId,\n" +
            "            retweet_tweet.quotedStatusId = r.quotedStatusId\n" +
            "        \n" +
            "        MERGE (tweet)-[:RETWEETED]->(retweet_tweet)\n" +
            "\n" +
            "        MERGE (user:User {screenName:r.user.screenName})\n" +
            "        SET\n" +
            "            user.name = r.user.name,\n" +
            "            user.id = r.user.id,\n" +
            "            user.location = r.user.location,\n" +
            "            user.isProtected = r.user.isProtected,\n" +
            "            user.followersCount = r.user.followersCount,\n" +
            "            user.friendsCount = r.user.friendsCount,\n" +
            "            user.createdAt = r.user.createdAt,\n" +
            "            user.favouritesCount = r.user.favouritesCount,\n" +
            "            user.utcOffset = r.user.utcOffset,\n" +
            "            user.lang = r.user.lang\n" +
            "\n" +
            "        MERGE (user)-[:POSTED]->(retweet_tweet)\n" +
            "\n" +
            "        FOREACH (h IN r.hashtagEntities |\n" +
            "            MERGE (tag:Tag {text:LOWER(h.text)})\n" +
            "            MERGE (tag)<-[:TAGGED]-(retweet_tweet)\n" +
            "        )\n" +
            "        FOREACH (u IN [u IN r.urlEntities WHERE u.expandedURL IS NOT NULL] |\n" +
            "            MERGE (url:Link {url:u.expandedURL})\n" +
            "            MERGE (retweet_tweet)-[:LINKED]->(url)\n" +
            "        )\n" +
            "        FOREACH (m IN [m IN r.mediaEntities WHERE m.id IS NOT NULL] |\n" +
            "            MERGE (media:Media {id:m.id})\n" +
            "            ON CREATE SET\n" +
            "              media.mediaURL= m.mediaURL\n" +
            "            MERGE (retweet_tweet)-[:CONTAINED]->(media)\n" +
            "        )\n" +
            "        FOREACH (m IN r.userMentionEntities |\n" +
            "            MERGE (mentioned:User {screenName:m.screenName})\n" +
            "            ON CREATE SET\n" +
            "              mentioned.name = m.name,\n" +
            "              mentioned.id = m.id\n" +
            "            MERGE (retweet_tweet)-[:MENTIONED]->(mentioned)\n" +
            "        )\n" +
            "        FOREACH ( a IN [ a IN [r.inReplyToStatusId] WHERE a IS NOT NULL AND a <> -1 ] |\n" +
            "            MERGE (reply_tweet_tweet:Tweet {id:a})\n" +
            "            MERGE (reply_tweet)-[:REPLIED_TO]->(reply_tweet_tweet)\n" +
            "        )\n" +
            "        \n" +
            "\n" +
            "        \n" +
            "    \n" +
            "    )"
            ;

    public Neo4jWriter(String neo4jUrl) throws URISyntaxException {
        URI boltUri = new URI(neo4jUrl);
        String[] authInfo = boltUri.getUserInfo().split(":");
        driver = GraphDatabase.driver(boltUri, AuthTokens.basic(authInfo[0], authInfo[1]));
    }

    public void init() {
        try (Session session = driver.session()) {
            session.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.screenName IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (t:Tag) ASSERT t.text IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE");
            session.run("CREATE CONSTRAINT ON (m:Media) ASSERT m.id IS UNIQUE");

        }
    }

    public void close() {
        driver.close();
    }

    /**
     * Inserts tweets to neo4j using STATEMENT
     * @param tweets
     * @param retries
     * @return
     */
    public int insert(List<String> tweets, int retries) {

        while (retries > 0) {
            try (Session session = driver.session()) {
                Gson gson = new GsonBuilder().create();

                List<Map> statuses = tweets.stream().map((s) -> gson.fromJson(s, Map.class)).collect(toList());
                long time = System.nanoTime();

                ResultSummary result = session.run(STATEMENT, Values.parameters("tweets", statuses)).consume();
                int created = result.counters().nodesCreated();


                System.out.println(created+" nodes in "+ TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-time)+" ms");
                System.out.flush();

                if(created == 0 ) {
                    System.out.println(" ###### couldn't insert tweet with id:" +statuses.get(0).get("id"));
                }

                return created;
            } catch (Exception e) {
                System.err.println(e.getClass().getSimpleName() + ":" + e.getMessage()+" retries left "+retries);
                retries--;
            }
        }
        return -1;
    }

}
