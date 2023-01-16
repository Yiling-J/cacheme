CREATE TABLE cacheme_data (
	id INTEGER NOT NULL,
	"key" VARCHAR(512),
	value BLOB,
	expire DATETIME,
	updated_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
	PRIMARY KEY (id),
	UNIQUE ("key")
);
CREATE INDEX ix_cacheme_data_expire ON cacheme_data (expire);
