-- This script swaps the temporary computed listing info table into
-- the final name, and then adds a number of indexes.

BEGIN TRANSACTION;

-- Swap the new table in.
DROP TABLE IF EXISTS computed_listing_info CASCADE;
ALTER TABLE computed_listing_info_tmp
    RENAME TO computed_listing_info;

-- Create an index for the important columns.
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
