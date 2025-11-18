

---Labs---

WITH blood_diff AS(
SELECT subject_id, hadm_id, specimen_id, charttime,
MAX(CASE WHEN itemid IN (51300, 51301, 51755) THEN valuenum ELSE NULL END) AS wbc,
MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils

FROM mimiciv_hosp.labevents WHERE itemid IN (51300, 51301, 51755, 52075, 51256) AND valuenum IS NOT NULL
GROUP BY subject_id, hadm_id, charttime, specimen_id
), blood_diff_filter AS (
	SELECT subject_id, hadm_id, charttime,
		wbc,
		CASE WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL THEN neutrophils * wbc ELSE neutrophils_abs END neutrophils_abs,
		neutrophils
	FROM blood_diff
)--SELECT * FROM blood_diff_filter;
, first_blood_diff_filter AS(
  SELECT subject_id, hadm_id, stay_id,
    MAX(wbc) wbc_max,
    MIN(wbc) wbc_min,
    MAX(neutrophils_abs) neutrophils_abs_max,
    MIN(neutrophils_abs) neutrophils_abs_min,
    MIN(neutrophils) neutrophils_min,
    MAX(neutrophils) neutrophils_max
  FROM blood_diff_filter RIGHT JOIN mimiciv_icu.icustays USING(subject_id, hadm_id)
  WHERE charttime BETWEEN intime - INTERVAL '6 HOURS'
    AND intime + INTERVAL '1 DAYS'
  GROUP BY subject_id, hadm_id, stay_id
)
--SELECT * FROM first_blood_diff_filter;

, chemistry AS(
  SELECT subject_id, hadm_id, specimen_id, charttime,
    MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE null END) creatinine,
    MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE null END) albumin
  FROM mimiciv_hosp.labevents WHERE itemid IN (50862, 50912) AND valuenum IS NOT NULL AND valuenum > 0
  GROUP BY subject_id, hadm_id, specimen_id, charttime
),
first_chemistry AS(
  SELECT subject_id, hadm_id, stay_id,
    MAX(creatinine) creatinine_max,
    MIN(creatinine) creatinine_min,
    MAX(albumin) albumin_max,
    MAX(albumin) albumin_min
  FROM chemistry RIGHT JOIN mimiciv_icu.icustays USING(subject_id, hadm_id)
  WHERE charttime BETWEEN intime - INTERVAL '6 HOURS'
    AND intime + INTERVAL '4 DAYS'
  GROUP BY subject_id, hadm_id, stay_id
)
SELECT subject_id, hadm_id, stay_id,
  wbc_max,
  albumin_min
FROM first_blood_diff_filter LEFT JOIN first_chemistry USING(subject_id, hadm_id, stay_id) WHERE albumin_min IS NOT NULL;






SELECT * FROM mimiciv_hosp.labevents
	



-----------
--albumin
SELECT * FROM mimiciv_hosp.labevents WHERE itemid = 50862; --g/dL
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%albumin%' AND fluid = 'Blood';--g/dL


--NPAR neutrophils % / albumin

SELECT * FROM mimiciv_hosp.labevents WHERE itemid = 52075; --k/uL (887.962)
SELECT * FROM mimiciv_hosp.labevents WHERE itemid = 51256; --% (1.611.767)

SELECT * FROM mimiciv_hosp.d_labitems WHERE itemid IN (52075, 51256);
SELECT * FROM mimiciv_hosp.labevents WHERE itemid IN (51300, 51301, 51755);

SELECT * FROM mimiciv_hosp.labevents WHERE specimen_id = 7278855;

SELECT * FROM mimiciv_hosp.d_labitems WHERE itemid IN (51146, 51200, 51221, 51222, 51244, 51248, 51249, 51250, 51254, 51256, 51265, 51277, 51279, 51301)


SELECT subject_id, hadm_id, specimen_id, charttime,
	MAX(CASE WHEN itemid IN (51300, 51301, 51755) THEN valuenum ELSE NULL END) AS wbc,
	MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
	MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
	FROM mimiciv_hosp.labevents WHERE itemid IN(51300, 51301, 51755, 52075, 51256) AND valuenum IS NOT NULL
	GROUP BY subject_id, hadm_id, specimen_id, charttime



SELECT * FROM mimiciv_hosp.d_labitems WHERE itemid IN (52075, 51256)


SELECT * FROM mimiciv_hosp.labevents WHERE itemid = 52075
	