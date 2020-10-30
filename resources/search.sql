-- This script sets up a computed table, which aggregates course information
-- from across the many tables. Furthermore, it creates a search function which
-- enables Hasura to use this table.

-- See this blog post for information on postgres full-text search:
-- http://rachbelaid.com/postgres-full-text-search-is-good-enough/

-- See this Hasura blog post for more information on the search function:
-- https://hasura.io/blog/full-text-search-with-hasura-graphql-api-postgres/

BEGIN;

-- encourage index usage
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET random_page_cost = 1;
SET seq_page_cost = 1;
SET enable_hashjoin = OFF;

DROP FUNCTION IF EXISTS search_listing_info;
DROP TABLE IF EXISTS computed_listing_info CASCADE;

CREATE TABLE computed_listing_info AS
WITH listing_info
    AS (
        SELECT listings.listing_id,
               listings.crn,
               listings.course_code,
               listings.subject,
               listings.number,
               listings.section,
           courses.*,
           (SELECT jsonb_agg(listings.course_code)
            FROM listings
            WHERE listings.course_id = courses.course_id
            GROUP BY listings.course_id) AS all_course_codes,
           coalesce((SELECT jsonb_agg(p.name)
            FROM course_professors
                     JOIN professors p on course_professors.professor_id = p.professor_id
            WHERE course_professors.course_id = courses.course_id
            GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_names,
           coalesce((SELECT jsonb_agg(json_build_object('name', p.name, 'email', p.email, 'average_rating', p.average_rating))
            FROM course_professors
                     JOIN professors p on course_professors.professor_id = p.professor_id
            WHERE course_professors.course_id = courses.course_id
            GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_info,
           (SELECT avg(p.average_rating)
            FROM course_professors
                     JOIN professors p on course_professors.professor_id = p.professor_id
            WHERE course_professors.course_id = courses.course_id
            GROUP BY course_professors.course_id) AS average_professor,
           (SELECT enrollment FROM evaluation_statistics
            WHERE evaluation_statistics.course_id = listings.course_id) as enrollment,
           -- 0 as enrolled,
           -- 0 as responses,
           -- 0 as declined,
           -- 0 as no_response
           (SELECT enrolled FROM evaluation_statistics
           WHERE evaluation_statistics.course_id = listings.course_id) as enrolled,
           (SELECT responses FROM evaluation_statistics
           WHERE evaluation_statistics.course_id = listings.course_id) as responses,
           (SELECT declined FROM evaluation_statistics
           WHERE evaluation_statistics.course_id = listings.course_id) as declined,
           (SELECT no_response FROM evaluation_statistics
           WHERE evaluation_statistics.course_id = listings.course_id) as no_response
        FROM listings
        JOIN courses on listings.course_id = courses.course_id
    )
SELECT listing_id,
       crn,
       subject,
       number,
       section,
       course_code,
       course_id,
       season_code,
       title,
       description,
       school,
       credits,
       times_summary,
       times_by_day,
       locations_summary,
       requirements,
       syllabus_url,
       extra_info,
       all_course_codes,
       professor_names, -- TODO: remove
       professor_info,
       average_professor,
       average_rating,
       average_workload,
       enrollment,
       enrolled,
       responses,
       declined,
       no_response,
       to_jsonb(skills) as skills,
       to_jsonb(areas) as areas,
       (setweight(to_tsvector('english', title), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'C') ||
        setweight(to_tsvector('english', course_code), 'A') ||
        --setweight(jsonb_to_tsvector('english', all_course_codes, '"all"'), 'B') ||
        setweight(jsonb_to_tsvector('english', professor_names, '"all"'), 'B')
       ) AS info
FROM listing_info
ORDER BY course_code, course_id ;

-- Create an index for basically every column.
ALTER TABLE computed_listing_info ADD FOREIGN KEY (course_id) REFERENCES courses (course_id);
ALTER TABLE computed_listing_info ADD FOREIGN KEY (listing_id) REFERENCES listings (listing_id);
CREATE INDEX idx_computed_listing_course_id ON computed_listing_info (course_id);
CREATE UNIQUE INDEX idx_computed_listing_listing_id ON computed_listing_info (listing_id);
CREATE INDEX idx_computed_listing_search ON computed_listing_info USING gin (info);
CREATE INDEX idx_computed_listing_order_def ON computed_listing_info (course_code ASC, course_id ASC);
CREATE INDEX idx_computed_listing_skills ON computed_listing_info USING gin (skills);
CREATE INDEX idx_computed_listing_areas ON computed_listing_info USING gin (areas);
CREATE INDEX idx_computed_listing_season ON computed_listing_info (season_code);
CREATE INDEX idx_computed_listing_season_hash ON computed_listing_info USING hash (season_code);

CREATE OR REPLACE FUNCTION search_listing_info(query text)
RETURNS SETOF computed_listing_info AS $$
BEGIN
    CASE
        WHEN websearch_to_tsquery('english', query) <@ ''::tsquery THEN
            -- If the query is completely empty, then we want to return everything,
            -- rather than the default behavior of matching nothing.
            RETURN QUERY SELECT * FROM computed_listing_info
            ORDER BY course_code, course_id ;
        ELSE
            RETURN QUERY SELECT *
            FROM computed_listing_info
            WHERE info @@ websearch_to_tsquery('english', query)
            ORDER BY course_code, course_id ;
            --ORDER BY
            --        -- If the ranking is above 0.5, then the query matches an "A" level (title or course_code),
            --        -- in which case we want to order by the course code and id. If the ranking is below 0.5,
            --        -- then we simply use the course_code and course_id as a fallback for ordering.
            --        -- This way, searches for a specific department will have that department first, followed
            --        -- by any matches on other metadata.
            --        LEAST(0.5, ts_rank(info, websearch_to_tsquery('english', query))) DESC,
            --        course_code, course_id ;
    END CASE;
END;
$$ language plpgsql stable;

COMMIT;
