package main

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// sqlite schema for tracking images, blobs, and activations.
// uses wal mode for better concurrency and busy_timeout to handle lock contention.
const schema = `
-- images: logical container images indexed by digest
CREATE TABLE IF NOT EXISTS images(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  digest TEXT NOT NULL UNIQUE,        -- content hash for idempotency
  base_lv_id INTEGER,                  -- thin volume id for base layer (null if not created)
  complete BOOLEAN NOT NULL DEFAULT 0, -- all blobs fetched and unpacked
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  prepared BOOLEAN NOT NULL DEFAULT 0
);

-- blobs: content-addressed storage for layer tarballs
CREATE TABLE IF NOT EXISTS blobs(
  digest TEXT PRIMARY KEY,             -- sha256 of blob content
  etag TEXT UNIQUE,                    -- s3 etag for deduplication
  size_bytes INTEGER,
  local_path TEXT,                     -- path in blobs/ directory
  complete BOOLEAN NOT NULL DEFAULT 0, -- successfully downloaded
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- image-blob relationships: links images to their constituent layers
CREATE TABLE IF NOT EXISTS image_blobs(
  image_id INTEGER NOT NULL,
  blob_digest TEXT NOT NULL,
  sequence INTEGER NOT NULL,           -- layer ordering (0, 1, 2, ...)
  PRIMARY KEY(image_id, sequence),
  UNIQUE(image_id, blob_digest)
);

-- activations: snapshots created from base images
CREATE TABLE IF NOT EXISTS activations(
  id INTEGER PRIMARY KEY,
  image_id INTEGER NOT NULL,
  snap_lv_id INTEGER UNIQUE,           -- thin snapshot volume id
  mount_path TEXT,                     -- where snapshot is mounted
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- locks: simple key-value store for distributed locking
CREATE TABLE IF NOT EXISTS locks(
  k TEXT PRIMARY KEY,
  v TEXT
);
`

// initdb creates and initializes the sqlite database.
// enables wal mode for concurrent access and sets a 5-second busy timeout.
func InitDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "fsm.db?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	// create tables if they don't exist
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
