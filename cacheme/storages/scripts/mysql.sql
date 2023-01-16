CREATE TABLE cacheme_data (
	id INTEGER NOT NULL AUTO_INCREMENT,
	`key` VARCHAR(512),
	value BLOB,
	expire DATETIME(6),
	updated_at DATETIME(6) DEFAULT now(6),
	PRIMARY KEY (id),
	UNIQUE (`key`)
);
CREATE INDEX ix_cacheme_data_expire ON cacheme_data (expire);
