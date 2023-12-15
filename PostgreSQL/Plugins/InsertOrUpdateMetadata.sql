CREATE OR REPLACE FUNCTION InsertOrUpdateMetadata(resourceIds BIGINT[],
                                                  metadataTypes INTEGER[], 
                                                  metadataValues TEXT[],
                                                  revisions INTEGER[])
RETURNS VOID AS $body$
BEGIN
  	FOR i IN 1 .. ARRAY_LENGTH(resourceIds, 1) LOOP
		-- RAISE NOTICE 'Parameter %: % % %', i, resourceIds[i], metadataTypes[i], metadataValues[i];
		INSERT INTO Metadata VALUES(resourceIds[i], metadataTypes[i], metadataValues[i], revisions[i]) ON CONFLICT DO NOTHING;
	END LOOP;
  
END;
$body$ LANGUAGE plpgsql;