CREATE TABLE metadata (
  id SERIAL PRIMARY KEY,
  last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

UPDATE metadata SET last_update = NOW() WHERE id = 1;
