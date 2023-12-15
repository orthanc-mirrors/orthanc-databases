-- uninstall FastTotalSize & FastCountResources
DROP TRIGGER IF EXISTS AttachedFileIncrementSize ON AttachedFiles;
DROP TRIGGER IF EXISTS AttachedFileDecrementSize ON AttachedFiles;
DROP TRIGGER IF EXISTS CountResourcesTracker ON Resources;

-- this table stores all changes that needs to be performed to the GlobalIntegers table
-- This way, each transaction can add row independently in this table without having to lock
-- any row (which was the case with previous FastTotalSize).
-- These changes will be applied at regular interval by an external thread or when someone
-- requests the statistics
CREATE TABLE IF NOT EXISTS GlobalIntegersChanges(
    key INTEGER,
    value BIGINT);

CREATE OR REPLACE FUNCTION UpdateSingleStatistic(
    IN statisticsKey INTEGER,
    OUT newValue BIGINT
) AS $body$
BEGIN

  -- Delete the current changes, sum them and update the GlobalIntegers row.
  -- New rows can be added in the meantime, they won't be deleted or summed.
  WITH deleted_rows AS (
      DELETE FROM GlobalIntegersChanges
      WHERE GlobalIntegersChanges.key = statisticsKey
      RETURNING value
  )
  UPDATE GlobalIntegers
  SET value = value + (
      SELECT COALESCE(SUM(value), 0)
      FROM deleted_rows
  )
  WHERE GlobalIntegers.key = statisticsKey
  RETURNING value INTO newValue;

END;
$body$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION UpdateStatistics(
  OUT patientsCount BIGINT,
  OUT studiesCount BIGINT,
  OUT seriesCount BIGINT,
  OUT instancesCount BIGINT,
  OUT totalCompressedSize BIGINT,
  OUT totalUncompressedSize BIGINT
) AS $body$
BEGIN

  SELECT UpdateSingleStatistic(0) INTO totalCompressedSize;
  SELECT UpdateSingleStatistic(1) INTO totalUncompressedSize;
  SELECT UpdateSingleStatistic(2) INTO patientsCount;
  SELECT UpdateSingleStatistic(3) INTO studiesCount;
  SELECT UpdateSingleStatistic(4) INTO seriesCount;
  SELECT UpdateSingleStatistic(5) INTO instancesCount;

END;
$body$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION IncrementResourcesTrackerFunc()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(new.resourceType + 2, 1);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION DecrementResourcesTrackerFunc()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(old.resourceType + 2, -1);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION AttachedFileIncrementSizeFunc()
RETURNS TRIGGER AS $body$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(0, new.compressedSize);
  INSERT INTO GlobalIntegersChanges VALUES(1, new.uncompressedSize);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION AttachedFileDecrementSizeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  INSERT INTO GlobalIntegersChanges VALUES(0, -old.compressedSize);
  INSERT INTO GlobalIntegersChanges VALUES(1, -old.uncompressedSize);
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;



CREATE TRIGGER AttachedFileIncrementSize
AFTER INSERT ON AttachedFiles
FOR EACH ROW
EXECUTE PROCEDURE AttachedFileIncrementSizeFunc();

CREATE TRIGGER AttachedFileDecrementSize
AFTER DELETE ON AttachedFiles
FOR EACH ROW
EXECUTE PROCEDURE AttachedFileDecrementSizeFunc();

CREATE TRIGGER IncrementResourcesTracker
AFTER INSERT ON Resources
FOR EACH ROW
EXECUTE PROCEDURE IncrementResourcesTrackerFunc();

CREATE TRIGGER DecrementResourcesTracker
AFTER DELETE ON Resources
FOR EACH ROW
EXECUTE PROCEDURE DecrementResourcesTrackerFunc();
