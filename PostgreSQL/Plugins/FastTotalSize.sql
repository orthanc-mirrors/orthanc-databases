CREATE TABLE GlobalIntegers(
       key INTEGER PRIMARY KEY,
       value BIGINT);

INSERT INTO GlobalIntegers
SELECT 0, CAST(COALESCE(SUM(compressedSize), 0) AS BIGINT) FROM AttachedFiles;

INSERT INTO GlobalIntegers
SELECT 1, CAST(COALESCE(SUM(uncompressedSize), 0) AS BIGINT) FROM AttachedFiles;



CREATE FUNCTION AttachedFileIncrementSizeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  UPDATE GlobalIntegers SET value = value + new.compressedSize WHERE key = 0;
  UPDATE GlobalIntegers SET value = value + new.uncompressedSize WHERE key = 1;
  RETURN NULL;
END;
$body$ LANGUAGE plpgsql;

CREATE FUNCTION AttachedFileDecrementSizeFunc() 
RETURNS TRIGGER AS $body$
BEGIN
  UPDATE GlobalIntegers SET value = value - old.compressedSize WHERE key = 0;
  UPDATE GlobalIntegers SET value = value - old.uncompressedSize WHERE key = 1;
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
