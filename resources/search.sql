-- This script sets up a computed table, which aggregates course information
-- from across the many tables. Furthermore, it creates a search function which
-- enables Hasura to use this table.

-- See this blog post for information on postgres full-text search:
-- http://rachbelaid.com/postgres-full-text-search-is-good-enough/

-- See this Hasura blog post for more information on the search function:
-- https://hasura.io/blog/full-text-search-with-hasura-graphql-api-postgres/

DROP FUNCTION IF EXISTS search_listing_info;
DROP TABLE IF EXISTS computed_listing_info CASCADE;

CREATE TABLE computed_listing_info AS
WITH listing_info
    AS (
        SELECT listings.listing_id,
           listings.course_code,
           courses.*,
           (SELECT jsonb_agg(listings.course_code)
            FROM listings
            WHERE listings.course_id = courses.course_id
            GROUP BY listings.course_id) AS all_course_codes,
           coalesce((SELECT jsonb_agg(p.name)
            FROM course_professors
                     JOIN professors p on course_professors.professor_id = p.professor_id
            WHERE course_professors.course_id = courses.course_id
            GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_names
        FROM listings
        JOIN courses on listings.course_id = courses.course_id
    )
SELECT listing_id,
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
       all_course_codes,
       professor_names,
       average_rating,
       average_workload,
       to_jsonb(skills) as skills,
       to_jsonb(areas) as areas,
       (setweight(to_tsvector('english', title), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'C') ||
        setweight(to_tsvector('english', course_code), 'A') ||
        setweight(jsonb_to_tsvector('english', all_course_codes, '"all"'), 'B') ||
        setweight(jsonb_to_tsvector('english', professor_names, '"all"'), 'B')
       ) AS info
FROM listing_info ;

-- Create an index for basically every column.
ALTER TABLE computed_listing_info ADD FOREIGN KEY (course_id) REFERENCES courses (course_id);
ALTER TABLE computed_listing_info ADD FOREIGN KEY (listing_id) REFERENCES listings (listing_id);
CREATE INDEX idx_computed_listing_search ON computed_listing_info USING gin (info);
CREATE INDEX idx_computed_listing_skills ON computed_listing_info USING gin (skills);
CREATE INDEX idx_computed_listing_areas ON computed_listing_info USING gin (areas);
CREATE INDEX idx_computed_listing_season ON computed_listing_info (season_code);

CREATE OR REPLACE FUNCTION search_listing_info(query text)
RETURNS SETOF computed_listing_info AS $$
BEGIN
    CASE
        WHEN websearch_to_tsquery('english', query) <@ ''::tsquery THEN
            RETURN QUERY SELECT * FROM computed_listing_info ORDER BY course_code ;
        ELSE
            RETURN QUERY SELECT *
            FROM computed_listing_info
            WHERE info @@ websearch_to_tsquery('english', query)
            ORDER BY ts_rank(info, websearch_to_tsquery('english', query)) DESC ;
    END CASE;
END;
$$ language plpgsql stable;