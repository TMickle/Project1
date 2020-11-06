----------
-------- Q1
--1. Which English wikipedia article got the most traffic on October 20?


wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-20201020-{00..23}0000.gz

CREATE EXTERNAL TABLE PAGEVIEW
    (DOMAIN_CODE STRING,
    PAGE_TITLE STRING,
    COUNT_VIEWS INT,
    TOTAL_RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' '
    LOCATION '/user/tmickle/project1/oct20/oct20';

    CREATE TABLE PAGEVIEW_COUNTS
    AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS) AS total_views FROM PAGEVIEW
    WHERE DOMAIN_CODE LIKE "en%" 
    GROUP BY PAGE_TITLE;

--- Top 10 total views for Oct 20 --
    select * from PAGEVIEW_COUNTS
    order by total_views desc
    limit 10;

---------------------------------

-- Q2
-- 2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article? -- We are answering for Sept only because of limited HD space and computing power.

wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-202010{00..31}-{00..23}0000.gz

wget https://dumps.wikimedia.org/other/clickstream/2020-09/clickstream-enwiki-2020-09.tsv.gz


--- Tables for pageviews for all of sept
CREATE EXTERNAL TABLE PAGEVIEW_SEPT
    (DOMAIN_CODE STRING,
    PAGE_TITLE STRING,
    COUNT_VIEWS INT,
    TOTAL_RESPONSE_SIZE INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ' '
    LOCATION '/user/tmickle/project1/sept/sept';

--Accumalated table with total views for page of Sept.
    CREATE TABLE TOTAL_PAGEVIEW_COUNTS_SEPT
    AS SELECT PAGE_TITLE, SUM(COUNT_VIEWS) AS total_views FROM PAGEVIEW_SEPT
    WHERE DOMAIN_CODE LIKE "en%" 
    GROUP BY PAGE_TITLE;

-- Table for Clickstream data for Sept.
    CREATE EXTERNAL TABLE CLICKSTREAM
    (PREV STRING,
    CURR STRING,
    TYPE STRING,
    N INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    LOCATION '/user/tmickle/project1/clickstreams';

--- Aggregation of links followed from a Page
    create table CLICKSTREAM_LINKS_FOLLOWED
    AS SELECT prev, SUM(n) AS links_followed FROM CLICKSTREAM WHERE TYPE = "link"
    group by prev;

-- Aggregation of number of external pages that links to Pages.  
    create table CLICKSTREAM_EXTERNAL_LINKS  AS select curr, sum(n) as external_links from clickstream where type = "external" group by curr order by external_links desc;

    --  First possible solution (we get many outliners though, with 4x the followed links to views of pages)
  SELECT PAGE_TITLE, total_views, links_follwed, ((links_follwed/ total_views) * 100) AS precentage_followed_link 
    FROM TOTAL_PAGEVIEW_COUNTS_SEPT 
    inner join clickstream_links_followed
    on prev = PAGE_TITLE
     where total_views > 100000
    order by precentage_followed_link desc
    limit 20;

--- Because of the outliners of the previous approach; We stick to using one set of data as our source of measurement. (Clickstream data)
--- Table for total views from the clickstream data by adding external links + interal links (We leave out 'other' because they are for searches to a page / spoofed )
create table clickstream_total_views AS
select curr as title, (external_links + links_follwed) as total from CLICKSTREAM_EXTERNAL_LINKS
inner join clickstream_links_followed
on prev = curr
order by total desc;

-- Our final solution Query.
-- We no longer get over 100% results; though out total views from this approach no longer match the total views total from the a TOTAL_PAGEVIEW_COUNTS_SEPT.
   create table links_followed_ratio AS SELECT title, total, links_follwed, ((links_follwed/ total) * 100) AS precentage_followed_link 
    FROM clickstream_total_views 
    inner join clickstream_links_followed
    on title = prev
    order by precentage_followed_link desc;

select * from links_followed_ratio where links_follwed > 200000  order by precentage_followed_link desc limit 25;

select * from links_followed_ratio  order by precentage_followed_link desc limit 25;


---------------------------------------------------
----------- Q3
--  What series of wikipedia articles, starting with [Hotel California](https://en.wikipedia.org/wiki/Hotel_California), keeps the largest fraction of its readers clicking on internal links?  This is similar to (2), but you should continue the analysis past the first article.

--- One possible way to iterate the results of our query. Very ugly though

    select * from CLICKSTREAM
    where prev in (
    select curr from CLICKSTREAM
    where prev in ( 
    SELECT curr from CLICKSTREAM
    where prev in 
    ( SELECT curr from CLICKSTREAM
    WHERE prev = "Hotel_California" AND type = "link"
    order by n desc limit 1) 
    order by n desc limit 1) 
    order by n desc limit 1) 
    order by n desc limit 1 ;


   -- Hotel_California -> 2222 Hotel_California_(Eagles_album)  -> 2127 The_Long_Run_(album)  -> 1322 Eagles_Live -> 1136 Eagles_Greatest_Hits,_Vol._2  ->   996 The_Very_Best_of_the_Eagles   -> 892  Hell_Freezes_Over  

   -- Hotel_California -> 2222 Hotel_California_(Eagles_album)
    SELECT prev, curr, type, n from CLICKSTREAM
    WHERE prev = "Hotel_California" AND type = "link"
    order by n desc
    limit 1;

-- Hotel_California_(Eagles_album) -> 2127 The_Long_Run_(album)   
    SELECT prev, curr, n from CLICKSTREAM
    WHERE prev = "Hotel_California_(Eagles_album)" AND type = "link"
    order by n desc
    limit 1;
-- The_Long_Run_(album)   -> 1322 Eagles_Live 
    SELECT prev, curr, n from CLICKSTREAM
    WHERE prev = "The_Long_Run_(album)" AND type = "link"
    order by n desc
    limit 1;

--  Eagles_Live  -> 1136 Eagles_Greatest_Hits,_Vol._2  
    SELECT prev, curr, n from CLICKSTREAM
    WHERE prev = "Eagles_Live" AND type = "link"
    order by n desc
    limit 1;

-- Eagles_Greatest_Hits,_Vol._2  | The_Very_Best_of_the_Eagles  | 996
     SELECT prev, curr, n from CLICKSTREAM
    WHERE prev = "Eagles_Greatest_Hits,_Vol._2" AND type = "link"
    order by n desc
    limit 1;

-- The_Very_Best_of_the_Eagles  | Hell_Freezes_Over  | 892  |
    SELECT prev, curr, n from CLICKSTREAM
    WHERE prev = "The_Very_Best_of_the_Eagles" AND type = "link"
    order by n desc
    limit 1;



-- the revisions for october 2020  
----- 2020-10.enwiki.2020-10.tsv.bz2 

create table revisions (
                WIKI_DB STRING, 
                EVENT_ENTITY STRING,
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP STRING,
                EVENT_COMMENT STRING,
                EVENT_USER_ID BIGINT,
                EVENT_USER_TEXT_HISTORICAL STRING,
                EVENT_USER_TEXT STRING,
                EVENT_USER_BLOCKS_HISTORICAL STRING,
                EVENT_USER_BLOCKS ARRAY<STRING>,
                EVENT_USER_GROUPS_HISTORICAL ARRAY<STRING>,
                EVENT_USER_GROUPS ARRAY<STRING>,
                event_user_is_bot_by_historical ARRAY<STRING>,
                event_user_is_bot_by ARRAY<STRING>,
                event_user_is_created_by_self BOOLEAN,
                event_user_is_created_by_system BOOLEAN,
                event_user_is_created_by_peer BOOLEAN,
                event_user_is_anonymous BOOLEAN,
                event_user_registration_timestamp STRING,
                event_user_creation_timestamp STRING,
                event_user_first_edit_timestamp STRING,
                event_user_revision_count BIGINT,
                event_user_seconds_since_previous_revision BIGINT,
                page_id BIGINT,
                page_title_historical STRING,
                page_title STRING,
                page_namespace_historical INT,
                page_namespace_is_content_historical BOOLEAN,
                page_namespace INT,
                page_namespace_is_content BOOLEAN,
                page_is_redirect BOOLEAN,
                page_is_deleted BOOLEAN,
                page_creation_timestamp STRING,
                page_first_edit_timestamp STRING,
                page_revision_count BIGINT,
                page_seconds_since_previous_revision BIGINT,
                user_id BIGINT,
                user_text_historical STRING,
                user_text STRING,
                user_blocks_historical ARRAY<STRING>,
                user_blocks ARRAY<STRING>,
                user_groups_historical ARRAY<STRING>,
                user_groups ARRAY<String>,
                user_is_bot_by_historical ARRAY<STRING>,
                user_is_bot_by Array<STRING>,
                user_is_created_by_self BOOLEAN,
                user_is_created_by_system boolean,
                user_is_created_by_peer BOOLEAN,
                user_is_anonymous boolean,
                user_registration_timestamp String,
                user_creation_timestamp STRING,
                user_first_edit_timestamp STRING,
                revision_id bigint,
                revision_parent_id bigint,
                revision_minor_edit boolean,
                revision_deleted_parts Array<String>,
                revision_deleted_parts_are_suppressed boolean,
                revision_text_bytes bigint,
                revision_text_bytes_diff bigint,
                revision_text_sha1 string,
                revision_content_model string,
                revision_content_format string,
                revision_is_deleted_by_page_deletion boolean,
                revision_deleted_by_page_deletion_timestamp string,
                revision_is_identity_reverted boolean,
                revision_first_identity_reverting_revision_id bigint,
                revision_seconds_to_identity_revert bigint,
                revision_is_identity_revert boolean,
                revision_is_from_before_page_creation boolean,
                revision_tags Array<string>
                )
            ROW FORMAT DELIMITED 
            FIELDS TERMINATED BY '\t'
            location "/user/tmickle/project1/wikiHistory";

            --Q4 4. 
-- Find an example of an English wikipedia article that is relatively more popular in the UK.  Find the same for the US and Australia.


           -- 10 - 2 pm
         create table UK_top_articles as  select  page_title, count(page_title) as n
             from revisions 
             where (event_entity = "page" or event_entity = "revision")
             and hour( event_timestamp) > 10 and hour(event_timestamp) < 14
             group by page_title
             order by n desc
              limit 100;

        -- e coast 12 - 6pm w coast 9am -3pm
            create table US_top_articles as  select  page_title, count(page_title) as n
             from revisions 
             where (event_entity = "page" or event_entity = "revision")
             and hour( event_timestamp) > 17 and hour(event_timestamp) < 23
             group by page_title
             order by n desc
              limit 100;

              create table Aus_top_articles as select  page_title, count(page_title) as n
             from revisions 
             where (event_entity = "page" or event_entity = "revision")
             and hour( event_timestamp) > 3 and hour(event_timestamp) < 9
             group by page_title
             order by n desc
              limit 100;

           

              select UK_top_articles.page_title, FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3) as average, (UK_top_articles.n / FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3)) as deviation  from UK_top_articles 
              inner join US_top_articles 
              on UK_top_articles.page_title = US_top_articles.page_title
              inner join Aus_top_articles 
              on UK_top_articles.page_title = Aus_top_articles.page_title
              order by deviation desc ;

              select US_top_articles.page_title, FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3) as average, (US_top_articles.n / FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3)) as deviation  from UK_top_articles 
              inner join US_top_articles 
              on UK_top_articles.page_title = US_top_articles.page_title
              inner join Aus_top_articles 
              on UK_top_articles.page_title = Aus_top_articles.page_title
              order by deviation desc ;

              select Aus_top_articles.page_title, 
              FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3) as average, 
              (Aus_top_articles.n / FLOOR((UK_top_articles.n + US_top_articles.n + Aus_top_articles.n)/3)) as deviation  
              from UK_top_articles 
              inner join US_top_articles 
              on UK_top_articles.page_title = US_top_articles.page_title
              inner join Aus_top_articles 
              on UK_top_articles.page_title = Aus_top_articles.page_title
              order by deviation desc ;


    -- Q5. Analyze how many users will see the average vandalized wikipedia page before the offending edit is reversed.

    -- Average time to revert  

    select Count(*) AS Total_Revisions, 
        Round(AVG(revision_seconds_to_identity_revert)) AS averageSecondsToRevert ,
        Round(AVG(revision_seconds_to_identity_revert)/60) AS averageMinToRevert,
        Round(AVG(revision_seconds_to_identity_revert)/3600) AS averageHourToRevert,
        Round(AVG(revision_seconds_to_identity_revert)/86400, 3) AS averageDayToRevert
     from revisions 
    where revision_is_identity_reverted  = true;

    -- Thought about taking the average view of a page and comparing then the time that a vandalized page might stay.  
    -- The average view for a page is about 12 views through out a month of sept. Just sanity check that  seems too low to then compare with our average revision times with. We would need to take a finer approach. 
    -- While the Average revision times given, are a good base to work from, the difference between high traffic pages vs low traffic pages evidently will swing in wide margins. 

    select count(*) as NUM_PAGES, (average(total_views)) from TOTAL_PAGEVIEW_COUNTS_SEPT; 


-------------------------------------------

-- revisions / page 
    create table Top_Contrib as select  event_user_id, sum(revision_text_bytes) as revisionByteSize from revisions
    where not event_entity = "user" 
    and EVENT_TYPE = "create" 
    and event_user_id > 0
    group by event_user_id
    order by revisionByteSize desc
    limit 20 ; 

    select event_user_text, Sum(revisionByteSize) as revisionByteSize  from Top_Contrib
    inner join revisions
    on Top_Contrib.event_user_id = revisions.event_user_id
    group by event_user_text
    order by revisionByteSize desc
    limit 20;