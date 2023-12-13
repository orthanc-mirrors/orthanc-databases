CREATE OR REPLACE FUNCTION CreateInstance(
  IN patient TEXT,
  IN study TEXT,
  IN series TEXT,
  IN instance TEXT,
  OUT isNewPatient BIGINT,
  OUT isNewStudy BIGINT,
  OUT isNewSeries BIGINT,
  OUT isNewInstance BIGINT,
  OUT patientKey BIGINT,
  OUT studyKey BIGINT,
  OUT seriesKey BIGINT,
  OUT instanceKey BIGINT) AS $body$

DECLARE
  patientSeq BIGINT;
  countRecycling BIGINT;

BEGIN
	isNewPatient := 1;
	isNewStudy := 1;
	isNewSeries := 1;
	isNewInstance := 1;

	BEGIN
        INSERT INTO "resources" VALUES (DEFAULT, 0, patient, NULL);
    EXCEPTION
        WHEN unique_violation THEN
            isNewPatient := 0;
    END;
    SELECT internalid INTO patientKey FROM "resources" WHERE publicId=patient AND resourcetype = 0;

	BEGIN
        INSERT INTO "resources" VALUES (DEFAULT, 1, study, patientKey);
    EXCEPTION
        WHEN unique_violation THEN
            isNewStudy := 0;
    END;
    SELECT internalid INTO studyKey FROM "resources" WHERE publicId=study AND resourcetype = 1;

	BEGIN
	    INSERT INTO "resources" VALUES (DEFAULT, 2, series, studyKey);
    EXCEPTION
        WHEN unique_violation THEN
            isNewSeries := 0;
    END;
	SELECT internalid INTO seriesKey FROM "resources" WHERE publicId=series AND resourcetype = 2;

  	BEGIN
		INSERT INTO "resources" VALUES (DEFAULT, 3, instance, seriesKey);
    EXCEPTION
        WHEN unique_violation THEN
            isNewInstance := 0;
    END;
    SELECT internalid INTO instanceKey FROM "resources" WHERE publicId=instance AND resourcetype = 3;   

  IF isNewInstance > 0 THEN
    -- Move the patient to the end of the recycling order
    SELECT seq FROM PatientRecyclingOrder WHERE patientId = patientKey INTO patientSeq;

    IF NOT (patientSeq IS NULL) THEN
       -- The patient is not protected
       SELECT COUNT(*) FROM (SELECT * FROM PatientRecyclingOrder WHERE seq >= patientSeq LIMIT 2) AS tmp INTO countRecycling;
       IF countRecycling = 2 THEN
          -- The patient was not at the end of the recycling order
          DELETE FROM PatientRecyclingOrder WHERE seq = patientSeq;
          INSERT INTO PatientRecyclingOrder VALUES(DEFAULT, patientKey);
       END IF;
    END IF;
  END IF;  
END;

$body$ LANGUAGE plpgsql;
