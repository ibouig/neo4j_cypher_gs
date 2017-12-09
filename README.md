# neo4j_cypher_gs

UNWIND {tweets} AS t
            WITH t,
            t.entities AS e,
            t.user AS u,
            t.retweeted_status AS retweet
            WHERE t.id is not null 
            MERGE (tweet:Tweet {id:t.id})
            SET tweet.text = t.text,
                tweet.created = t.created_at,
                tweet.favorites = t.favorite_count
            MERGE (user:User {screen_name:u.screen_name})
            SET user.name = u.name,
                user.location = u.location,
                user.followers = u.followers_count,
                user.following = u.friends_count,
                user.statuses = u.statuses_count,
                user.profile_image_url = u.profile_image_url
            MERGE (user)-[:POSTED]->(tweet)
            FOREACH (h IN e.hashtags |
              MERGE (tag:Tag {name:LOWER(h.text)})
              MERGE (tag)<-[:TAGGED]-(tweet)
            )
            FOREACH (u IN [u IN e.urls WHERE u.expanded_url IS NOT NULL] |
              MERGE (url:Link {url:u.expanded_url})
              MERGE (tweet)-[:LINKED]->(url)
            )
            FOREACH (m IN e.user_mentions |
              MERGE (mentioned:User {screen_name:m.screen_name})
              ON CREATE SET mentioned.name = m.name
              MERGE (tweet)-[:MENTIONED]->(mentioned)
            ) 
            FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |
              MERGE (reply_tweet:Tweet {id:r})
              MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
            )
            FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |
                MERGE (retweet_tweet:Tweet {id:retweet_id})
                MERGE (tweet)-[:RETWEETED]->(retweet_tweet)
            )