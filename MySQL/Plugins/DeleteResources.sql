DROP PROCEDURE IF EXISTS DeleteResources;

CREATE PROCEDURE DeleteResources(
    IN p_id BIGINT
)
BEGIN
    DECLARE v_internalId BIGINT@
    DECLARE done INT DEFAULT FALSE@
	DECLARE cur1 CURSOR FOR
		SELECT internalId FROM DeletedResources@
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = TRUE@
	set done=FALSE@	
    -- Create a CTE to hold the temporary data ???
    -- WITH DeletedResources AS (
    --    SELECT internalId, resourceType, publicId
    --    FROM Resources
    -- )

	CREATE TEMPORARY TABLE DeletedResources SELECT * FROM (
		SELECT internalId, resourceType, publicId FROM Resources WHERE internalId=p_id OR parentId=p_id 
			OR parentId IN (SELECT internalId FROM Resources WHERE parentId=p_id) 
			OR parentId IN (SELECT internalId FROM Resources WHERE parentId IN (SELECT internalId FROM Resources WHERE parentId=p_id))) AS t@

	OPEN cur1@
	REPEAT
		FETCH cur1 INTO v_internalId@
		IF NOT done THEN
		DELETE FROM Resources WHERE internalId=v_internalId@
		END IF@
	UNTIL done END REPEAT@
	CLOSE cur1@

END;