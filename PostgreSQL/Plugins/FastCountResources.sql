-- https://wiki.postgresql.org/wiki/Count_estimate

INSERT INTO GlobalIntegers
SELECT 2, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 0;  -- Count patients

INSERT INTO GlobalIntegers
SELECT 3, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 1;  -- Count studies

INSERT INTO GlobalIntegers
SELECT 4, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 2;  -- Count series

INSERT INTO GlobalIntegers
SELECT 5, CAST(COALESCE(COUNT(*), 0) AS BIGINT) FROM Resources WHERE resourceType = 3;  -- Count instances


CREATE OR REPLACE FUNCTION CountResourcesTrackerFunc()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE GlobalIntegers SET value = value + 1 WHERE key = new.resourceType + 2;
    RETURN new;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE GlobalIntegers SET value = value - 1 WHERE key = old.resourceType + 2;
    RETURN old;
  END IF;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER CountResourcesTracker
AFTER INSERT OR DELETE ON Resources
FOR EACH ROW
EXECUTE PROCEDURE CountResourcesTrackerFunc();
