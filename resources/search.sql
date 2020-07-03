-- This script sets up a materialized view, which aggregates course information
-- from across the many tables. Furthermore, it creates a search function which
-- enables Hasura to use this table.

-- See this blog post for information on postgres full-text search:
-- http://rachbelaid.com/postgres-full-text-search-is-good-enough/

-- See this Hasura blog post for more information on the search function:
-- https://hasura.io/blog/full-text-search-with-hasura-graphql-api-postgres/

DROP MATERIALIZED VIEW IF EXISTS course_info_table CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS course_info_table AS
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
       times_summary,
       locations_summary,
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
FROM course_info
;

-- REFRESH MATERIALIZED VIEW course_info_table;

CREATE INDEX idx_course_search ON course_info_table USING gin(info) ;
-- TODO: create index on basically every column

CREATE FUNCTION search_course_info(query text)
RETURNS SETOF course_info_table AS $$
    SELECT *
    FROM course_info_table
    WHERE info @@ websearch_to_tsquery('english', query)
    ORDER BY ts_rank(info, websearch_to_tsquery('english', query)) DESC
$$ language sql stable;

CREATE OR REPLACE FUNCTION search_courses(query text)
RETURNS SETOF courses AS $$
    SELECT courses.*
    FROM courses
    JOIN course_info_table cit on courses.course_id = cit.course_id
    WHERE cit.info @@ websearch_to_tsquery('english', query)
    ORDER BY ts_rank(cit.info, websearch_to_tsquery('english', query)) DESC
$$ language sql stable;