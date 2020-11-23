-- This script sets up a computed table, which aggregates course information
-- from across the many tables.

-- Encourage index usage.
SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET random_page_cost = 1;
SET seq_page_cost = 1;
SET enable_hashjoin = OFF;

-- Create temporary listings table.
DROP TABLE IF EXISTS computed_listing_info_tmp;
CREATE TABLE computed_listing_info_tmp AS
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
                GROUP BY listings.course_id)                                 AS all_course_codes,
               coalesce((SELECT jsonb_agg(p.name)
                         FROM course_professors
                                  JOIN professors p on course_professors.professor_id = p.professor_id
                         WHERE course_professors.course_id = courses.course_id
                         GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_names,
               coalesce((SELECT jsonb_agg(json_build_object('name', p.name, 'email', p.email, 'average_rating',
                                                            p.average_rating))
                         FROM course_professors
                                  JOIN professors p on course_professors.professor_id = p.professor_id
                         WHERE course_professors.course_id = courses.course_id
                         GROUP BY course_professors.course_id), '[]'::jsonb) AS professor_info,
               (SELECT avg(p.average_rating)
                FROM course_professors
                         JOIN professors p on course_professors.professor_id = p.professor_id
                WHERE course_professors.course_id = courses.course_id
                GROUP BY course_professors.course_id)                        AS average_professor,
               coalesce((SELECT jsonb_agg(f.flag_text)
                         FROM course_flags
                                  JOIN flags f on course_flags.flag_id = f.flag_id
                         WHERE course_flags.course_id = courses.course_id
                         GROUP BY course_flags.course_id), '[]'::jsonb)      AS flag_info,
               (SELECT enrollment
                FROM evaluation_statistics
                WHERE evaluation_statistics.course_id = listings.course_id)  AS enrollment,
               (SELECT enrolled
                FROM evaluation_statistics
                WHERE evaluation_statistics.course_id = listings.course_id)  AS enrolled,
               (SELECT responses
                FROM evaluation_statistics
                WHERE evaluation_statistics.course_id = listings.course_id)  AS responses,
               (SELECT declined
                FROM evaluation_statistics
                WHERE evaluation_statistics.course_id = listings.course_id)  AS declined,
               (SELECT no_response
                FROM evaluation_statistics
                WHERE evaluation_statistics.course_id = listings.course_id)  AS no_response
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
       professor_names,
       professor_info,
       average_professor,
       flag_info,
       fysem,
       regnotes,
       rp_attr,
       classnotes,
       final_exam,
       average_rating,
       average_workload,
       (average_rating - average_workload) as average_gut_rating,
       last_offered_course_id,
       last_enrollment_course_id,
       last_enrollment,
       last_enrollment_season_code,
       last_enrollment_same_professors,
       enrollment,
       enrolled,
       responses,
       declined,
       no_response,
       to_jsonb(skills) as skills,
       to_jsonb(areas)  as areas
FROM listing_info
ORDER BY course_code, course_id;


BEGIN TRANSACTION;

-- Swap the new table in and update the search function.
DROP TABLE IF EXISTS computed_listing_info CASCADE;
ALTER TABLE computed_listing_info_tmp
    RENAME TO computed_listing_info;

-- Create an index for basically every column.
ALTER TABLE computed_listing_info
    ADD FOREIGN KEY (course_id) REFERENCES courses (course_id);
ALTER TABLE computed_listing_info
    ADD FOREIGN KEY (listing_id) REFERENCES listings (listing_id);
CREATE INDEX idx_computed_listing_course_id ON computed_listing_info (course_id);
CREATE UNIQUE INDEX idx_computed_listing_listing_id ON computed_listing_info (listing_id);
CREATE INDEX idx_computed_listing_order_def ON computed_listing_info (course_code ASC, course_id ASC);
CREATE INDEX idx_computed_listing_skills ON computed_listing_info USING gin (skills);
CREATE INDEX idx_computed_listing_areas ON computed_listing_info USING gin (areas);
CREATE INDEX idx_computed_listing_season ON computed_listing_info (season_code);
CREATE INDEX idx_computed_listing_season_hash ON computed_listing_info USING hash (season_code);

COMMIT;
