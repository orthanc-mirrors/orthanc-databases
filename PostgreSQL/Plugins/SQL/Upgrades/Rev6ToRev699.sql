-- Adding a PK to these 2 table to allow pg_repack to process these tables, enabling reclaiming disk space and defragmenting the tables.

ALTER TABLE InvalidChildCounts ADD COLUMN pk BIGSERIAL PRIMARY KEY;
ALTER TABLE GlobalIntegersChanges ADD COLUMN pk BIGSERIAL PRIMARY KEY;

-- Adding the queues timeout
ALTER TABLE Queues ADD COLUMN reservedUntil TIMESTAMP DEFAULT NULL;