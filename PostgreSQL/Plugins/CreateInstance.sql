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
    exception
        when unique_violation then
            isNewPatient := 0;
    end;
    select internalid into patientKey from "resources" where publicId=patient and resourcetype = 0;

	BEGIN
        INSERT INTO "resources" VALUES (DEFAULT, 1, study, patientKey);
    exception
        when unique_violation then
            isNewStudy := 0;
    end;
    select internalid into studyKey from "resources" where publicId=study and resourcetype = 1;

	BEGIN
	    INSERT INTO "resources" VALUES (DEFAULT, 2, series, studyKey);
    exception
        when unique_violation then
            isNewSeries := 0;
    end;
	select internalid into seriesKey from "resources" where publicId=series and resourcetype = 2;

  	BEGIN
		INSERT INTO "resources" VALUES (DEFAULT, 3, instance, seriesKey);
    exception
        when unique_violation then
            isNewInstance := 0;
    end;
    select internalid into instanceKey from "resources" where publicId=instance and resourcetype = 3;   

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
