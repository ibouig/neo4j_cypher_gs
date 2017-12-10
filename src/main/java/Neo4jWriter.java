import com.google.gson.Gson;
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
    static String STATEMENT = "UNWIND {tweets} AS t\n" +
            "WITH t,\n" +
            "    t.entities AS e,\n" +
            "    t.user AS u,\n" +
            "    t.retweeted_status AS retweet,\n" +
            "    t.place AS p,\n" +
            "    t.geo AS g\n" +
            "\n" +
            "WHERE t.id is not null\n" +
            "MERGE (tweet:Tweet {id:t.id})\n" +
            "SET\n" +
            "    tweet.text = t.text,\n" +
            "    tweet.created_at = t.created_at,\n" +
            "    tweet.source = t.source,\n" +
            "    tweet.truncated = t.truncated,\n" +
            "    tweet.in_reply_to_status_id = t.in_reply_to_status_id,\n" +
            "    tweet.in_reply_to_user_id = t.in_reply_to_user_id,\n" +
            "    tweet.in_reply_to_screen_name = t.in_reply_to_screen_name,\n" +
            "    tweet.contributors = t.contributors,\n" +
            "    tweet.is_quote_status = t.is_quote_status,\n" +
            "    tweet.quote_count = t.quote_count,\n" +
            "    tweet.reply_count = t.reply_count,\n" +
            "    tweet.retweet_count = t.retweet_count,\n" +
            "    tweet.favorite_count = t.favorite_count\n" +
            "\n" +
            "MERGE (place:Place)\n" +
            "SET\n" +
            "    place.id = CASE place WHEN p.id IS NULL THEN p.id ELSE -1 END,\n" +
            "    place.url = t.place.url,\n" +
            "    place.place_type = t.place.place_type,\n" +
            "    place.name = t.place.name,\n" +
            "    place.full_name = t.place.full_name,\n" +
            "    place.country_code = t.place.country_code,\n" +
            "    place.country = t.place.country\n" +
            "MERGE (tweet)-[:TOOKPLACE]->(place)\n" +
            "\n" +
            "MERGE (user:User {screen_name:u.screen_name})\n" +
            "SET\n" +
            "    user.name = u.name,\n" +
            "    user.id = u.id,\n" +
            "    user.location = u.location,\n" +
            "    user.url = u.url,\n" +
            "    user.description = u.description,\n" +
            "    user.translator_type = u.translator_type,\n" +
            "    user.protected = u.protected,\n" +
            "    user.verified = u.verified,\n" +
            "    user.followers_count = u.followers_count,\n" +
            "    user.friends_count = u.friends_count,\n" +
            "    user.listed_count = u.listed_count,\n" +
            "    user.favourites_count = u.favourites_count,\n" +
            "    user.statuses_count = u.statuses_count,\n" +
            "    user.created_at = u.created_at,\n" +
            "    user.utc_offset = u.utc_offset,\n" +
            "    user.time_zone = u.time_zone,\n" +
            "    user.geo_enabled = u.geo_enabled,\n" +
            "    user.lang = u.lang,\n" +
            "    user.contributors_enabled = u.contributors_enabled,\n" +
            "    user.is_translator = u.is_translator,\n" +
            "    user.profile_background_color = u.profile_background_color,\n" +
            "    user.profile_background_image_url = u.profile_background_image_url,\n" +
            "    user.profile_background_image_url_https = u.profile_background_image_url_https,\n" +
            "    user.profile_background_tile = u.profile_background_tile,\n" +
            "    user.profile_link_color = u.profile_link_color,\n" +
            "    user.profile_sidebar_border_color = u.profile_sidebar_border_color,\n" +
            "    user.profile_sidebar_fill_color = u.profile_sidebar_fill_color,\n" +
            "    user.profile_text_color = u.profile_text_color,\n" +
            "    user.profile_use_background_image = u.profile_use_background_image,\n" +
            "    user.profile_image_url = u.profile_image_url,\n" +
            "    user.profile_image_url_https = u.profile_image_url_https,\n" +
            "    user.profile_banner_url = u.profile_banner_url,\n" +
            "    user.default_profile = u.default_profile,\n" +
            "    user.default_profile_image = u.default_profile_image,\n" +
            "    user.following = u.following,\n" +
            "    user.follow_request_sent = u.follow_request_sent,\n" +
            "    user.notifications = u.notifications\n" +
            "MERGE (user)-[:POSTED]->(tweet)\n" +
            "FOREACH (h IN e.hashtags |\n" +
            "    MERGE (tag:Tag {name:LOWER(h.text)})\n" +
            "    MERGE (tag)<-[:TAGGED]-(tweet)\n" +
            ")\n" +
            "FOREACH (u IN [u IN e.urls WHERE u.expanded_url IS NOT NULL] |\n" +
            "    MERGE (url:Link {url:u.expanded_url})\n" +
            "    MERGE (tweet)-[:LINKED]->(url)\n" +
            ")        \n" +
            "FOREACH (m IN e.user_mentions |\n" +
            "    MERGE (mentioned:User {screen_name:m.screen_name})\n" +
            "    ON CREATE SET mentioned.name = m.name\n" +
            "    MERGE (tweet)-[:MENTIONED]->(mentioned)\n" +
            ")\n" +
            "FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |\n" +
            "    MERGE (reply_tweet:Tweet {id:r})\n" +
            "    MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)\n" +
            ")\n" +
            "FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |\n" +
            "    MERGE (retweet_tweet:Tweet {id:retweet_id})\n" +
            "    MERGE (tweet)-[:RETWEETED]->(retweet_tweet)\n" +
            ")\n" +
            "\n" +
            "MERGE (geo:Geo)\n" +
            "SET\n" +
            "    geo.type=g.type,\n" +
            "    geo.coordinates = g.coordinates\n" +
            "    \n" +
            "MERGE (tweet)-[:GEOLOCALAZED]->(geo)\n" +
            "\n"
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
        while (retries > 0) {
            try (Session session = driver.session()) {
                Gson gson = new Gson();
                List<Map> statuses = tweets.stream().map((s) -> gson.fromJson(s, Map.class)).collect(toList());
                long time = System.nanoTime();

                ResultSummary result = session.run(STATEMENT, Values.parameters("tweets", statuses)).consume();
                int created = result.counters().nodesCreated();

                System.out.println(created+" in "+ TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-time)+" ms");
                System.out.flush();

                return created;
            } catch (Exception e) {
                System.err.println(e.getClass().getSimpleName() + ":" + e.getMessage()+" retries left "+retries);
                retries--;
            }
        }
        return -1;
    }

}
