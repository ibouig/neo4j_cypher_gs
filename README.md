# neo4j_cypher_gs

    UNWIND {tweets} AS t
    WITH t,
        t.entities AS e,
        t.user AS u,
        t.retweeted_status AS retweet,
        t.place AS p,
        t.geo AS g
    
    WHERE t.id is not null
    MERGE (tweet:Tweet {id:t.id})
    SET
        tweet.text = t.text,
        tweet.created_at = t.created_at,
        tweet.source = t.source,
        tweet.truncated = t.truncated,
        tweet.in_reply_to_status_id = t.in_reply_to_status_id,
        tweet.in_reply_to_user_id = t.in_reply_to_user_id,
        tweet.in_reply_to_screen_name = t.in_reply_to_screen_name,
        tweet.contributors = t.contributors,
        tweet.is_quote_status = t.is_quote_status,
        tweet.quote_count = t.quote_count,
        tweet.reply_count = t.reply_count,
        tweet.retweet_count = t.retweet_count,
        tweet.favorite_count = t.favorite_count
    
    MERGE (place:Place)
    SET
        place.id = CASE place WHEN p.id IS NULL THEN p.id ELSE -1 END,
        place.url = t.place.url,
        place.place_type = t.place.place_type,
        place.name = t.place.name,
        place.full_name = t.place.full_name,
        place.country_code = t.place.country_code,
        place.country = t.place.country
    MERGE (tweet)-[:TOOKPLACE]->(place)
    
    MERGE (user:User {screen_name:u.screen_name})
    SET
        user.name = u.name,
        user.id = u.id,
        user.location = u.location,
        user.url = u.url,
        user.description = u.description,
        user.translator_type = u.translator_type,
        user.protected = u.protected,
        user.verified = u.verified,
        user.followers_count = u.followers_count,
        user.friends_count = u.friends_count,
        user.listed_count = u.listed_count,
        user.favourites_count = u.favourites_count,
        user.statuses_count = u.statuses_count,
        user.created_at = u.created_at,
        user.utc_offset = u.utc_offset,
        user.time_zone = u.time_zone,
        user.geo_enabled = u.geo_enabled,
        user.lang = u.lang,
        user.contributors_enabled = u.contributors_enabled,
        user.is_translator = u.is_translator,
        user.profile_background_color = u.profile_background_color,
        user.profile_background_image_url = u.profile_background_image_url,
        user.profile_background_image_url_https = u.profile_background_image_url_https,
        user.profile_background_tile = u.profile_background_tile,
        user.profile_link_color = u.profile_link_color,
        user.profile_sidebar_border_color = u.profile_sidebar_border_color,
        user.profile_sidebar_fill_color = u.profile_sidebar_fill_color,
        user.profile_text_color = u.profile_text_color,
        user.profile_use_background_image = u.profile_use_background_image,
        user.profile_image_url = u.profile_image_url,
        user.profile_image_url_https = u.profile_image_url_https,
        user.profile_banner_url = u.profile_banner_url,
        user.default_profile = u.default_profile,
        user.default_profile_image = u.default_profile_image,
        user.following = u.following,
        user.follow_request_sent = u.follow_request_sent,
        user.notifications = u.notifications
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
    
    MERGE (geo:Geo)
    SET
        geo.type=g.type,
        geo.coordinates = g.coordinates
        
    MERGE (tweet)-[:GEOLOCALAZED]->(geo)



########

UNWIND {tweets} AS t
    WITH t,
        t.contributorsIDs as contributors,
        t.retweetedStatus AS retweetedStatus,
        t.userMentionEntities AS userMentionEntities,
        t.urlEntities AS urlEntities,
        t.hashtagEntities AS hashtagEntities,
        t.mediaEntities AS mediaEntities,
        t.symbolEntities AS symbolEntities,
        t.user AS u
    WHERE t.id is not null

    MERGE (tweet:Tweet {id:t.id})
    SET
        tweet.text = t.text,
        tweet.createdAt = t.createdAt,
        tweet.source = t.source,
        tweet.isTruncated = t.isTruncated,
        tweet.isFavorited = t.isFavorited,
        tweet.isRetweeted = t.isRetweeted,
        tweet.favoriteCount = t.favoriteCount,
        tweet.retweetCount = t.retweetCount,
        tweet.isPossiblySensitive = t.isPossiblySensitive,
        tweet.lang = t.lang,
        tweet.currentUserRetweetId = t.currentUserRetweetId,
        tweet.quotedStatusId = t.quotedStatusId

    MERGE (user:User {screenName:u.screenName})
    SET
        user.name = u.name,
        user.id = u.id,
        user.location = u.location,
        user.isProtected = u.isProtected,
        user.followersCount = u.followersCount,
        user.friendsCount = u.friendsCount,
        user.createdAt = u.createdAt,
        user.favouritesCount = u.favouritesCount,
        user.utcOffset = u.utcOffset,
        user.lang = u.lang

    MERGE (user)-[:POSTED]->(tweet)
    FOREACH (h IN hashtagEntities |
        MERGE (tag:Tag {text:LOWER(h.text)})
        MERGE (tag)<-[:TAGGED]-(tweet)
    )
    FOREACH (u IN [u IN urlEntities WHERE u.expandedURL IS NOT NULL] |
        MERGE (url:Link {url:u.expandedURL})
        MERGE (tweet)-[:LINKED]->(url)
    )
    FOREACH (m IN [m IN mediaEntities WHERE m.id IS NOT NULL] |
        MERGE (media:Media {id:m.id})
        ON CREATE SET
          media.mediaURL= m.mediaURL
        MERGE (tweet)-[:CONTAINED]->(media)
    )
    FOREACH (m IN userMentionEntities |
        MERGE (mentioned:User {screenName:m.screenName})
        ON CREATE SET
          mentioned.name = m.name,
          mentioned.id = m.id
        MERGE (tweet)-[:MENTIONED]->(mentioned)
    )
    FOREACH (r IN [r IN [t.inReplyToStatusId] WHERE r IS NOT NULL AND r <> -1 ] |
        MERGE (reply_tweet:Tweet {id:r})
        MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
    )
    FOREACH (r IN [r IN [retweetedStatus.id] WHERE r IS NOT NULL AND r <> -1 ] |
        MERGE (retweet_tweet:Tweet {id:retweet_id})
        MERGE (tweet)-[:RETWEETED]->(retweet_tweet)
    )
#########

UNWIND {tweets} AS t
    WITH t,
        t.contributorsIDs as contributors,
        t.retweetedStatus AS retweetedStatus,
        t.userMentionEntities AS userMentionEntities,
        t.urlEntities AS urlEntities,
        t.hashtagEntities AS hashtagEntities,
        t.mediaEntities AS mediaEntities,
        t.symbolEntities AS symbolEntities,
        t.user AS u
    WHERE t.id is not null

    MERGE (tweet:Tweet {id:t.id})
    SET
        tweet.text = t.text,
        tweet.createdAt = t.createdAt,
        tweet.source = t.source,
        tweet.isTruncated = t.isTruncated,
        tweet.isFavorited = t.isFavorited,
        tweet.isRetweeted = t.isRetweeted,
        tweet.favoriteCount = t.favoriteCount,
        tweet.retweetCount = t.retweetCount,
        tweet.isPossiblySensitive = t.isPossiblySensitive,
        tweet.lang = t.lang,
        tweet.currentUserRetweetId = t.currentUserRetweetId,
        tweet.quotedStatusId = t.quotedStatusId

    MERGE (user:User {screenName:u.screenName})
    SET
        user.name = u.name,
        user.id = u.id,
        user.location = u.location,
        user.isProtected = u.isProtected,
        user.followersCount = u.followersCount,
        user.friendsCount = u.friendsCount,
        user.createdAt = u.createdAt,
        user.favouritesCount = u.favouritesCount,
        user.utcOffset = u.utcOffset,
        user.lang = u.lang

    MERGE (user)-[:POSTED]->(tweet)
    FOREACH (h IN hashtagEntities |
        MERGE (tag:Tag {text:LOWER(h.text)})
        MERGE (tag)<-[:TAGGED]-(tweet)
    )
    FOREACH (u IN [u IN urlEntities WHERE u.expandedURL IS NOT NULL] |
        MERGE (url:Link {url:u.expandedURL})
        MERGE (tweet)-[:LINKED]->(url)
    )
    FOREACH (m IN [m IN mediaEntities WHERE m.id IS NOT NULL] |
        MERGE (media:Media {id:m.id})
        ON CREATE SET
          media.mediaURL= m.mediaURL
        MERGE (tweet)-[:CONTAINED]->(media)
    )
    FOREACH (m IN userMentionEntities |
        MERGE (mentioned:User {screenName:m.screenName})
        ON CREATE SET
          mentioned.name = m.name,
          mentioned.id = m.id
        MERGE (tweet)-[:MENTIONED]->(mentioned)
    )
    FOREACH (r IN [r IN [t.inReplyToStatusId] WHERE r IS NOT NULL AND r <> -1 ] |
        MERGE (reply_tweet:Tweet {id:r})
        MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)
    )
    

    FOREACH (r IN [r IN [retweetedStatus] WHERE r IS NOT NULL AND r <> -1 ] |
    
    
        MERGE (retweet_tweet:Tweet {id:r.id})
        SET
            retweet_tweet.text = r.text,
            retweet_tweet.createdAt = r.createdAt,
            retweet_tweet.source = r.source,
            retweet_tweet.isTruncated = r.isTruncated,
            retweet_tweet.isFavorited = r.isFavorited,
            retweet_tweet.isRetweeted = r.isRetweeted,
            retweet_tweet.favoriteCount = r.favoriteCount,
            retweet_tweet.retweetCount = r.retweetCount,
            retweet_tweet.isPossiblySensitive = r.isPossiblySensitive,
            retweet_tweet.lang = r.lang,
            retweet_tweet.currentUserRetweetId = r.currentUserRetweetId,
            retweet_tweet.quotedStatusId = r.quotedStatusId
        
        MERGE (tweet)-[:RETWEETED]->(retweet_tweet)

        MERGE (user:User {screenName:r.user.screenName})
        SET
            user.name = r.user.name,
            user.id = r.user.id,
            user.location = r.user.location,
            user.isProtected = r.user.isProtected,
            user.followersCount = r.user.followersCount,
            user.friendsCount = r.user.friendsCount,
            user.createdAt = r.user.createdAt,
            user.favouritesCount = r.user.favouritesCount,
            user.utcOffset = r.user.utcOffset,
            user.lang = r.user.lang

        MERGE (user)-[:POSTED]->(retweet_tweet)

        FOREACH (h IN r.hashtagEntities |
            MERGE (tag:Tag {text:LOWER(h.text)})
            MERGE (tag)<-[:TAGGED]-(retweet_tweet)
        )
        FOREACH (u IN [u IN r.urlEntities WHERE u.expandedURL IS NOT NULL] |
            MERGE (url:Link {url:u.expandedURL})
            MERGE (retweet_tweet)-[:LINKED]->(url)
        )
        FOREACH (m IN [m IN r.mediaEntities WHERE m.id IS NOT NULL] |
            MERGE (media:Media {id:m.id})
            ON CREATE SET
              media.mediaURL= m.mediaURL
            MERGE (retweet_tweet)-[:CONTAINED]->(media)
        )
        FOREACH (m IN r.userMentionEntities |
            MERGE (mentioned:User {screenName:m.screenName})
            ON CREATE SET
              mentioned.name = m.name,
              mentioned.id = m.id
            MERGE (retweet_tweet)-[:MENTIONED]->(mentioned)
        )
        FOREACH ( a IN [ a IN [r.inReplyToStatusId] WHERE a IS NOT NULL AND a <> -1 ] |
            MERGE (reply_tweet_tweet:Tweet {id:a})
            MERGE (reply_tweet)-[:REPLIED_TO]->(reply_tweet_tweet)
        )
    )