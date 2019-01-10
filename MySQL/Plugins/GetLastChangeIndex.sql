CREATE TABLE GlobalIntegers(
       property INTEGER PRIMARY KEY,
       value BIGINT
       );


INSERT INTO GlobalIntegers
SELECT 0, COALESCE(MAX(seq), 0) FROM Changes;


CREATE TRIGGER ChangeAdded
AFTER INSERT ON Changes
FOR EACH ROW
BEGIN
  UPDATE GlobalIntegers SET value = new.seq WHERE property = 0@
END;
