CREATE TABLE IF NOT EXISTS metadata (
  id SERIAL PRIMARY KEY,
  last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO metadata (id, last_update) VALUES (1, NOW())
ON CONFLICT (id) DO UPDATE SET last_update = EXCLUDED.last_update;
