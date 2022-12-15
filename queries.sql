SELECT username,
   posts,
   FIRST_VALUE(posts) OVER (
      PARTITION BY username 
      ORDER BY posts DESC
   ) AS 'fewest_posts'
FROM social_media;


SELECT
   username,
   posts,
   LAST_VALUE (posts) OVER (
      PARTITION BY username 
      ORDER BY posts
      RANGE BETWEEN UNBOUNDED PRECEDING AND 
      UNBOUNDED FOLLOWING
    ) most_posts
FROM
    social_media;
    
    
    --RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING specifies the frame for our window function as the current partition and thus returns the highest number of posts in one month for each user.


SELECT * FROM streams;

SELECT
  artist,
  week,
  streams_millions,
  LAG(streams_millions,1,0) OVER(
    PARTITION BY artist
    ORDER BY WEEK
  ) previous_week_streams
FROM
streams;




SELECT
   artist,
   week,
   streams_millions,
   LAG(streams_millions, 1, 0) OVER (
      ORDER BY week 
   ) previous_week_streams 
FROM
   streams 
WHERE
   artist = 'Lady Gaga';




SELECT
   artist,
   week,
   streams_millions,
   streams_millions - LAG(streams_millions, 1, streams_millions) OVER ( 
      ORDER BY week 
   ) streams_millions_change
FROM
   streams 
WHERE
   artist = 'Lady Gaga';



SELECT
   artist,
   week,
   streams_millions,
   streams_millions - LAG(streams_millions, 1, streams_millions) OVER (
    --  PARTITION BY artist
      ORDER BY week 
   ) AS "streams_millions_change",
   chart_position,
   LAG(chart_position,1,chart_position) OVER (
     PARTITION BY artist
     ORDER BY week
   ) - chart_position AS "chart_position_change"

FROM
   streams 
WHERE artist = "Lady Gaga";





SELECT
   artist,
   week,
   streams_millions,
   LEAD(streams_millions, 1,streams_millions) OVER (
      PARTITION BY artist
      ORDER BY week
   ) - streams_millions AS 'streams_millions_change',
   chart_position - LEAD(chart_position) OVER (
     PARTITION BY artist
     ORDER BY week
   ) AS "chart_position_change"

FROM
   streams;
WHERE artist = "Lady Gaga";


SELECT 
   NTILE(4) OVER (
      PARTITION BY week
      ORDER BY streams_millions DESC
   ) AS 'weekly_streams_group', 
   artist, 
   week,
   streams_millions
FROM
   streams;
