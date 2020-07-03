-- This script sets up a computed table, which aggregates course information
-- from across the many tables. Furthermore, it creates a search function which
-- enables Hasura to use this table.

-- See this blog post for information on postgres full-text search:
-- http://rachbelaid.com/postgres-full-text-search-is-good-enough/

-- See this Hasura blog post for more information on the search function:
-- https://hasura.io/blog/full-text-search-with-hasura-graphql-api-postgres/

DROP FUNCTION IF EXISTS search_course_info;
DROP TABLE IF EXISTS computed_course_info CASCADE;

CREATE TABLE computed_course_info AS
WITH course_info
    AS (
        SELECT courses.*,
               (SELECT jsonb_agg(listings.course_code)
                FROM listings
                WHERE listings.course_id = courses.course_id
                GROUP BY listings.course_id) AS course_codes,
               coalesce((SELECT jsonb_agg(p.name)
                FROM course_professors
                         JOIN professors p on course_professors.professor_id = p.professor_id
                WHERE course_professors.course_id = courses.course_id
                GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_names
        FROM courses
    )
SELECT course_id,
       season_code,
       title,
       description,
       school,
       credits,
       times_summary,
       times_by_day,
       locations_summary,
       requirements,
       course_codes,
       professor_names,
       average_rating,
       average_workload,
       to_jsonb(skills) as skills,
       to_jsonb(areas) as areas,
       (setweight(to_tsvector('english', title), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'C') ||
        setweight(jsonb_to_tsvector('english', course_codes, '"all"'), 'A') ||
        setweight(jsonb_to_tsvector('english', professor_names, '"all"'), 'B')
       ) AS info
FROM course_info ;

-- Create an index for basically every column.
ALTER TABLE computed_course_info ADD FOREIGN KEY (course_id) REFERENCES courses (course_id);
CREATE INDEX idx_computed_course_search ON computed_course_info USING gin (info);
CREATE INDEX idx_computed_course_skills ON computed_course_info USING gin (skills);
CREATE INDEX idx_computed_course_areas ON computed_course_info USING gin (areas);
CREATE INDEX idx_computed_course_season ON computed_course_info (season_code);

CREATE FUNCTION search_course_info(query text)
RETURNS SETOF computed_course_info AS $$
    SELECT *
    FROM computed_course_info
    WHERE info @@ websearch_to_tsquery('english', query)
    ORDER BY ts_rank(info, websearch_to_tsquery('english', query)) DESC
$$ language sql stable;
