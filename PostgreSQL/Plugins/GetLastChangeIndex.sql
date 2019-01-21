-- In PostgreSQL, the most straightforward query would be to run:

--   SELECT currval(pg_get_serial_sequence('Changes', 'seq'))".

-- Unfortunately, this raises the error message "currval of sequence
-- "changes_seq_seq" is not yet defined in this session" if no change
-- has been inserted before the SELECT. We thus track the sequence
-- index with a trigger.
-- http://www.neilconway.org/docs/sequences/

INSERT INTO GlobalIntegers
SELECT 6, CAST(COALESCE(MAX(seq), 0) AS BIGINT) FROM Changes;


CREATE FUNCTION InsertedChangeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  UPDATE GlobalIntegers SET value = new.seq WHERE key = 6;
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;


CREATE TRIGGER InsertedChange
AFTER INSERT ON Changes
FOR EACH ROW
EXECUTE PROCEDURE InsertedChangeFunc();
