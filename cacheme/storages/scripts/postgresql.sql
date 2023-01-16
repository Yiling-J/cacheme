CREATE TABLE cacheme_data (
	id SERIAL NOT NULL,
	key VARCHAR(512),
	value BYTEA,
	expire TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
	PRIMARY KEY (id),
	UNIQUE (key)
);
CREATE INDEX ix_cacheme_data_expire ON cacheme_data (expire);
