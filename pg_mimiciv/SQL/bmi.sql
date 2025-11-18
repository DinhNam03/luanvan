WITH weight AS (
  SELECT subject_id, hadm_id, stay_id, charttime,
         valuenum AS weight_kg,
         ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime ASC) AS rn
  FROM mimiciv_icu.chartevents
  WHERE itemid = 226512  -- Weight (kg)
    AND valuenum IS NOT NULL
),
height AS (
  SELECT subject_id, hadm_id, stay_id, charttime,
         valuenum AS height_cm,
         ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime ASC) AS rn
  FROM mimiciv_icu.chartevents
  WHERE itemid = 226730  -- Height (cm)
    AND valuenum IS NOT NULL
    AND valuenum > 0     -- tránh chia cho 0
),
bmi_calc AS (
  SELECT w.subject_id, w.hadm_id, w.stay_id,
         ROUND((w.weight_kg / POWER(h.height_cm/100.0, 2))::numeric, 2) AS bmi
  FROM weight w
  JOIN height h
    ON w.subject_id = h.subject_id
   AND w.stay_id = h.stay_id
   AND w.rn = 1
   AND h.rn = 1
),
comorb AS (
  SELECT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
)
SELECT b.subject_id, b.hadm_id, b.stay_id, b.bmi, c.diabetes, c.hypertension
FROM bmi_calc b
LEFT JOIN comorb c
  ON b.subject_id = c.subject_id
 AND b.hadm_id = c.hadm_id;





-- Định nghĩa comorbidities: diabetes và hypertension
SELECT DISTINCT subject_id, hadm_id,
    CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END AS diabetes,
    CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END AS hypertension
FROM mimiciv_hosp.diagnoses_icd;



