CREATE FUNCTION CreateInstance(
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
  SELECT internalId FROM Resources INTO instanceKey WHERE publicId = instance AND resourceType = 3;

  IF NOT (instanceKey IS NULL) THEN
    -- This instance already exists, stop here
    isNewInstance := 0;
  ELSE
    SELECT internalId FROM Resources INTO patientKey WHERE publicId = patient AND resourceType = 0;
    SELECT internalId FROM Resources INTO studyKey WHERE publicId = study AND resourceType = 1;
    SELECT internalId FROM Resources INTO seriesKey WHERE publicId = series AND resourceType = 2;

    IF patientKey IS NULL THEN
      -- Must create a new patient
      IF NOT (studyKey IS NULL AND seriesKey IS NULL AND instanceKey IS NULL) THEN
         RAISE EXCEPTION 'Broken invariant';
      END IF;

      INSERT INTO Resources VALUES (DEFAULT, 0, patient, NULL) RETURNING internalId INTO patientKey;
      isNewPatient := 1;
    ELSE
      isNewPatient := 0;
    END IF;
  
    IF (patientKey IS NULL) THEN
       RAISE EXCEPTION 'Broken invariant';
    END IF;

    IF studyKey IS NULL THEN
      -- Must create a new study
      IF NOT (seriesKey IS NULL AND instanceKey IS NULL) THEN
         RAISE EXCEPTION 'Broken invariant';
      END IF;

      INSERT INTO Resources VALUES (DEFAULT, 1, study, patientKey) RETURNING internalId INTO studyKey;
      isNewStudy := 1;
    ELSE
      isNewStudy := 0;
    END IF;

    IF (studyKey IS NULL) THEN
       RAISE EXCEPTION 'Broken invariant';
    END IF;

    IF seriesKey IS NULL THEN
      -- Must create a new series
      IF NOT (instanceKey IS NULL) THEN
         RAISE EXCEPTION 'Broken invariant';
      END IF;

      INSERT INTO Resources VALUES (DEFAULT, 2, series, studyKey) RETURNING internalId INTO seriesKey;
      isNewSeries := 1;
    ELSE
      isNewSeries := 0;
    END IF;
  
    IF (seriesKey IS NULL OR NOT instanceKey IS NULL) THEN
       RAISE EXCEPTION 'Broken invariant';
    END IF;

    INSERT INTO Resources VALUES (DEFAULT, 3, instance, seriesKey) RETURNING internalId INTO instanceKey;
    isNewInstance := 1;

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
