SELECT * FROM mimiciv_derived.tyg
--DROP TABLE mimiciv_derived.tyg

CREATE TABLE mimiciv_derived.tyg AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7481
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	-- WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
  -- SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  -- WHERE icd_code IN ('5712','5715','5716','K703','K717','K743','K744','K745','K746')
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CAST(CASE WHEN itemid = 50863 THEN valuenum END AS INTEGER)) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND valuenum > 0 
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         MAX(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid IN (50850,51000,51060,51803,51824,51950) THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL AND valuenum > 0
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,50850,51000,51060,51803,51824,51950,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
first_labs AS (
  SELECT ie.stay_id,
         AVG(wbc) AS wbc, AVG(rbc) AS rbc, AVG(rdw) AS rdw,
         AVG(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         AVG(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         AVG(potassium) AS potassium, AVG(creatinine) AS creatinine,
		 MIN(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         AVG(alt) AS alt, AVG(alp) AS alp, AVG(ast) AS ast,
         AVG(pt) AS pt, AVG(ptt) AS ptt, AVG(inr) AS inr,
         -- MAX(neutrophils_abs) AS neutrophils_abs_max,
         -- MAX(lymphocytes_abs) AS lymphocytes_abs_max,
         -- MAX(lymphocytes) AS lymphocytes_max,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
		 -- MAX(total_protein) AS total_protein
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   -- f.neutrophils_abs_max, 
	   -- f.lymphocytes_abs_max, 
	   -- f.lymphocytes_max,
	   -- (f.neutrophils_abs_max / f.lymphocytes_abs_max) AS NLR,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   -- f.total_protein,
	   CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis_3,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       LN((f.triglycerides * f.glucose) / 2.0) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.WBC IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;





---------------================tyg1==============----------------------

-- SELECT * FROM mimiciv_derived.tyg1
CREATE TABLE mimiciv_derived.tyg1 AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7481
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	-- WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
  -- SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  -- WHERE icd_code IN ('5712','5715','5716','K703','K717','K743','K744','K745','K746')
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CAST(CASE WHEN itemid = 50863 THEN valuenum END AS INTEGER)) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND valuenum > 0 
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         MAX(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid = 51000 THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL AND valuenum > 0
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,51000,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
first_labs AS (
  SELECT ie.stay_id,
         AVG(wbc) AS wbc, AVG(rbc) AS rbc, AVG(rdw) AS rdw,
         AVG(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         AVG(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         AVG(potassium) AS potassium, AVG(creatinine) AS creatinine,
		 MAX(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         AVG(alt) AS alt, AVG(alp) AS alp, AVG(ast) AS ast,
         AVG(pt) AS pt, AVG(ptt) AS ptt, AVG(inr) AS inr,
         -- MAX(neutrophils_abs) AS neutrophils_abs_max,
         -- MAX(lymphocytes_abs) AS lymphocytes_abs_max,
         -- MAX(lymphocytes) AS lymphocytes_max,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
		 -- MAX(total_protein) AS total_protein
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   -- f.neutrophils_abs_max, 
	   -- f.lymphocytes_abs_max, 
	   -- f.lymphocytes_max,
	   -- (f.neutrophils_abs_max / f.lymphocytes_abs_max) AS NLR,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   -- f.total_protein,
	   CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis_3,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       LN((f.triglycerides * f.glucose) / 2.0) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.WBC IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;







--------------=============tyg3==============--------------------

CREATE TABLE mimiciv_derived.tyg3 AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7481
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	-- WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
  -- SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  -- WHERE icd_code IN ('5712','5715','5716','K703','K717','K743','K744','K745','K746')
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CAST(CASE WHEN itemid = 50863 THEN valuenum END AS INTEGER)) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND valuenum > 0 
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         MAX(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid IN (50850,51000,51060,51803,51824,51950) THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL AND valuenum > 0
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,50850,51000,51060,51803,51824,51950,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
first_labs AS (
  SELECT ie.stay_id,
         AVG(wbc) AS wbc, AVG(rbc) AS rbc, AVG(rdw) AS rdw,
         AVG(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         AVG(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         AVG(potassium) AS potassium, AVG(creatinine) AS creatinine,
		 MIN(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         AVG(alt) AS alt, AVG(alp) AS alp, AVG(ast) AS ast,
         AVG(pt) AS pt, AVG(ptt) AS ptt, AVG(inr) AS inr,
         -- MAX(neutrophils_abs) AS neutrophils_abs_max,
         -- MAX(lymphocytes_abs) AS lymphocytes_abs_max,
         -- MAX(lymphocytes) AS lymphocytes_max,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
		 -- MAX(total_protein) AS total_protein
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   -- f.neutrophils_abs_max, 
	   -- f.lymphocytes_abs_max, 
	   -- f.lymphocytes_max,
	   -- (f.neutrophils_abs_max / f.lymphocytes_abs_max) AS NLR,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   -- f.total_protein,
	   CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis_3,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       LN((f.triglycerides * f.glucose) / 2.0) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.WBC IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;




















-----------------------------------------------

SELECT * FROM mimiciv_derived.tyg
--DROP TABLE mimiciv_derived.tyg
--CREATE TABLE mimiciv_derived.tyg AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7481
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	-- WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CAST(CASE WHEN itemid = 50863 THEN valuenum END AS INTEGER)) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND valuenum > 0 
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         MAX(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid IN (50850,51000,51060,51803,51824,51950) THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL AND valuenum > 0
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,50850,51000,51060,51803,51824,51950,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
first_labs AS (
  SELECT ie.stay_id,
         MAX(wbc) AS wbc, AVG(rbc) AS rbc, MAX(rdw) AS rdw,
         MAX(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         MAX(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         MAX(potassium) AS potassium, MAX(creatinine) AS creatinine,
		 MIN(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         MAX(alt) AS alt, MAX(alp) AS alp, MAX(ast) AS ast,
         MAX(pt) AS pt, MAX(ptt) AS ptt, MAX(inr) AS inr,
         -- MAX(neutrophils_abs) AS neutrophils_abs_max,
         -- MAX(lymphocytes_abs) AS lymphocytes_abs_max,
         -- MAX(lymphocytes) AS lymphocytes_max,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
		 -- MAX(total_protein) AS total_protein
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   -- f.neutrophils_abs_max, 
	   -- f.lymphocytes_abs_max, 
	   -- f.lymphocytes_max,
	   -- (f.neutrophils_abs_max / f.lymphocytes_abs_max) AS NLR,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   -- f.total_protein,
	   CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis_3,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       LN((f.triglycerides * f.glucose) / 2.0) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       -- CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d
       -- CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.WBC IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;




















-----------==============----------------

SELECT * FROM mimiciv_derived.tyg
-- DROP TABLE mimiciv_derived.tyg
CREATE TABLE mimiciv_derived.tyg AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7510
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	-- WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
  -- SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  -- WHERE icd_code IN ('5712','5715','5716','K703','K717','K743','K744','K745','K746')
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CASE WHEN itemid = 50863 THEN valuenum ELSE NULL END) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND (valuenum > 0 OR itemid = 50868)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         AVG(CAST(ROUND(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS INTEGER)) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid IN (50850,51000,51060,51803,51824,51950) THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
  	AND (valuenum > 0 OR itemid = 50868)
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,50850,51000,51060,51803,51824,51950,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
-- lab2_test AS (
-- 	SELECT 
-- 		subject_id, hadm_id, charttime, wbc,
-- 		-- Bạch cầu trung tính tuyệt đối
-- 		CASE WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL THEN neutrophils * wbc ELSE neutrophils_abs END AS neutrophils_abs,
-- 		neutrophils,
-- 		-- Bạch cầu lympho tuyệt đối
-- 		CASE WHEN lymphocytes_abs IS NULL AND lymphocytes IS NOT NULL THEN lymphocytes * wbc ELSE lymphocytes_abs END AS lymphocytes_abs,
-- 		lymphocytes,
-- 		-- NLR = Neutrophils_abs / Lymphocytes_abs
-- 		CASE WHEN lymphocytes_abs IS NULL OR lymphocytes_abs = 0 THEN NULL ELSE 
-- 				(CASE WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL AND wbc IS NOT NULL 
-- 					THEN (neutrophils * wbc) / lymphocytes_abs
-- 					ELSE neutrophils_abs / lymphocytes_abs 
-- 				END)
-- 		END AS NLR
-- 	FROM lab_2

-- ),
first_labs AS (
  SELECT ie.stay_id,
         MAX(wbc) AS wbc, AVG(rbc) AS rbc, MAX(rdw) AS rdw,
         MAX(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         MAX(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         MAX(potassium) AS potassium, MAX(creatinine) AS creatinine,
		 MIN(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         MAX(alt) AS alt, MAX(alp) AS alp, MAX(ast) AS ast,
         MAX(pt) AS pt, MAX(ptt) AS ptt, MAX(inr) AS inr,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT DISTINCT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.dod, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis_3,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       ROUND(LN((f.triglycerides * f.glucose) / 2.0)::numeric, 2) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.wbc IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;














------------++++++++++============___________________-------------------









------------------


SELECT subject_id, hadm_id, itemid, label, specimen_id
	FROM mimiciv_hosp.labevents JOIN mimiciv_hosp.d_labitems USING (itemid) WHERE specimen_id = 92819861
	
SELECT subject_id, hadm_id, itemid, label, specimen_id
	FROM mimiciv_hosp.labevents JOIN mimiciv_hosp.d_labitems USING (itemid) WHERE specimen_id = 2704548
	
SELECT subject_id, hadm_id, itemid, label, specimen_id
	FROM mimiciv_hosp.labevents JOIN mimiciv_hosp.d_labitems USING (itemid) WHERE specimen_id = 80570618
wbc,rbc,rdw,Hemoglobin,platelet,Neutrophils

SELECT subject_id, hadm_id, itemid, label, specimen_id
	FROM mimiciv_hosp.labevents JOIN mimiciv_hosp.d_labitems USING (itemid) WHERE specimen_id = 81013553
alt,alp,ast,

SELECT subject_id, hadm_id, itemid, specimen_id
	FROM mimiciv_hosp.labevents WHERE itemid = 50931


SELECT subject_id, hadm_id, itemid, label, specimen_id
	FROM mimiciv_hosp.labevents JOIN mimiciv_hosp.d_labitems USING (itemid) WHERE itemid = 50931



--white blood cell count (WBC): itemid: 51300
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%wbc%'

--red blood cell count (RBC): itemid: 52170
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%rbc%';

--red blood cell distribution width (RDW): itemid: 51277
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%rdw%';

--hemoglobin (Hemoglobin): itemid: 51222
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%hemoglobin%';

--platelet count (Platelets): itemid: 51265
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%platelet%';

-- anion gap (Anion gap): itemid: 50868, 52500
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%anion gap%';

--bicarbonate (Bicarbonate): itemid: 50882
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%bicarbonate%';

--calcium (Calcium): itemid: 50893
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%calcium%';

--chloride (Chloride): itemid: 50902, 52535
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%chloride%';

--potassium (Potassium): itemid: 50971, 52610
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%potassium%';

--creatinine (Creatinine): itemid: 50912, 52546
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%creatinine%';

--alanine aminotransferase (ALT): itemid: 50861
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%alt%';

--alkaline phosphatase (ALP): itemid: 50863, 
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%alkaline phosphatase%';

--aspartate aminotransferase (AST): itemid: 50878, 53086
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%ast%';

--prothrombin time (PT): itemid: 51274, 52921
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%pt%';

--partial activated thromboplastin time (PTT): itemid: 51275, 52923
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%ptt%';

----international normalized ratio (INR): itemid: 51237, 51675
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%inr%';

--glucose: itemid: 50931, 52569
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%glucose%';

--triglyceride: itemid: 51000
SELECT * FROM mimiciv_hosp.d_labitems WHERE LOWER(label) LIKE '%triglycerides%';

50850,51000,51060,51803,51824,51950


SELECT * FROM mimiciv_hosp.labevents WHERE itemid IN (51244, 51245);

--DROP TABLE mimiciv_derived.tryglc;
CREATE TABLE mimiciv_derived.tryglc AS
SELECT * FROM mimiciv_hosp.labevents WHERE itemid = 51000 AND hadm_id IS NOT NULL;

CREATE TABLE mimiciv_derived.partient_tryglc AS
SELECT v.subject_id, MAX(t.valuenum) ma, MIN(t.valuenum) mi FROM mimiciv_derived.viem v JOIN mimiciv_derived.tryglc t ON v.subject_id = t.subject_id AND v.hadm_id = t.hadm_id 
WHERE t.valuenum IS NOT NULL GROUP BY v.subject_id




SELECT * FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
 










SELECT * FROM mimiciv_derived.tyg
-- DROP TABLE mimiciv_derived.tyg
CREATE TABLE mimiciv_derived.tyg AS
WITH ap AS (
  -- Bệnh nhân viêm tụy cấp: 7510
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
),
ap_icu AS (
  -- Thông tin ICU: 74843
  	SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age, race,
         ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
  	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	WHERE los >= 1 OR los IS NULL
),
ap_first_icu AS (
  -- Chỉ lấy lần nhập ICU đầu tiên
  	SELECT * FROM ap_icu WHERE rn = 1
) 
-- SELECT * FROM ap_first_icu
,
sepsis AS (
  -- SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  -- WHERE icd_code IN ('5712','5715','5716','K703','K717','K743','K744','K745','K746')
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  	WHERE icd_code IN (
    '99592','67020','67022','67024','77181',
    'A021','A227','A267','A327','A40','A409','A41','A4150',
    'A4159','A418','A4189','A427','A5486','B377','O85',
    'P36','P368','P369','R652','R6520','R6521'
  )

),
aki AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5846','5847','5848','5849','N17','N170','N171','N172','N178','N179')
),
chronic_kidney_disease AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851','5852','5853','5854','5855','5856','5859','N18','N181',
  					'N182','N183','N1830','N1831','N1832','N184','N185','N186','N189')
),
comorbidity AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN k.subject_id IS NULL THEN 0 ELSE 1 END AS aki,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
  FROM ap_first_icu a
  LEFT JOIN sepsis s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki k    ON k.subject_id = a.subject_id AND k.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id
),
crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents
  WHERE itemid IN (227290,230177,225956)
),
mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('9670','9671','9672','5A1935Z','5A1945Z','5A1955Z')
),
ercp AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd
  WHERE icd_code IN ('5110','5213')
),
vasopressin AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents
  WHERE itemid = 222315
),
treatments AS (
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN e.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu a
  LEFT JOIN crrt c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
  LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
  LEFT JOIN ercp e ON a.subject_id = e.subject_id AND a.hadm_id = e.hadm_id
  LEFT JOIN vasopressin v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
),
labs AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 50861 THEN valuenum ELSE NULL END) AS alt,
         MAX(CASE WHEN itemid = 50863 THEN valuenum ELSE NULL END) AS alp,
         MAX(CASE WHEN itemid = 50878 THEN valuenum ELSE NULL END) AS ast,
         MAX(CASE WHEN itemid = 51274 THEN valuenum ELSE NULL END) AS pt,
         MAX(CASE WHEN itemid = 51275 THEN valuenum ELSE NULL END) AS ptt,
         MAX(CASE WHEN itemid = 51237 THEN valuenum ELSE NULL END) AS inr
  FROM mimiciv_hosp.labevents
  WHERE itemid IN (50861,50863,50878,51274,51275,51237) AND valuenum IS NOT NULL AND (valuenum > 0 OR itemid = 50868)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_1 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
  		 MAX(CASE WHEN itemid = 50868 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS aniongap,
         AVG(CAST(ROUND(CASE WHEN itemid = 50882 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS INTEGER)) AS bicarbonate,
         MAX(CASE WHEN itemid = 50893 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS calcium,
         MAX(CASE WHEN itemid = 50902 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS chloride,
         MAX(CASE WHEN itemid = 50971 AND valuenum <= 30 THEN valuenum ELSE NULL END) AS potassium,
         MAX(CASE WHEN itemid = 50912 AND valuenum <= 150 THEN valuenum ELSE NULL END) AS creatinine,
         MAX(CASE WHEN itemid = 50931 AND valuenum <= 10000 THEN valuenum ELSE NULL END) AS glucose,
         MAX(CASE WHEN itemid IN (50850,51000,51060,51803,51824,51950) THEN valuenum ELSE NULL END) AS triglycerides,
         MAX(CASE WHEN itemid = 50862 AND valuenum <= 10 THEN valuenum ELSE NULL END) AS albumin,
         MAX(CASE WHEN itemid = 51006 AND valuenum <= 300 THEN valuenum ELSE NULL END) AS bun
		 -- MAX(CASE WHEN itemid = 50976 AND valuenum <= 20 THEN valuenum ELSE NULL END) AS total_protein

  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
  	AND (valuenum > 0 OR itemid = 50868)
    AND itemid IN (50868,50882,50893,50902,50971,50912,50931,50850,51000,51060,51803,51824,51950,50862,51006, 50976)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
lab_2 AS (
  SELECT subject_id, hadm_id, charttime,specimen_id,
         MAX(CASE WHEN itemid = 51301 THEN valuenum ELSE NULL END) AS wbc,
         MAX(ROUND(CASE WHEN itemid = 51279 THEN valuenum END::numeric, 2)) AS rbc,
         MAX(CASE WHEN itemid = 51277 THEN valuenum ELSE NULL END) AS rdw,
         MAX(CASE WHEN itemid = 51222 THEN valuenum ELSE NULL END) AS hemoglobin,
         MAX(CASE WHEN itemid = 51265 THEN valuenum ELSE NULL END) AS platelets,
		 -- MAX(CASE WHEN itemid = 51133 THEN valuenum WHEN itemid = 52769 THEN valuenum / 1000.0 ELSE NULL END) AS lymphocytes_abs,
		 -- MAX(CASE WHEN itemid IN (51244, 51245) THEN valuenum ELSE NULL END) AS lymphocytes,
		 -- MAX(CASE WHEN itemid = 52075 THEN valuenum ELSE NULL END) AS neutrophils_abs,
         MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils
  FROM mimiciv_hosp.labevents
  WHERE hadm_id IS NOT NULL AND valuenum IS NOT NULL
    AND itemid IN (51301,51279,51277,51222,51265,51133,52769,51244,51245,52075,51256)
  GROUP BY subject_id, hadm_id, charttime, specimen_id
),
-- lab2_test AS (
-- 	SELECT 
-- 		subject_id, hadm_id, charttime, wbc,
-- 		-- Bạch cầu trung tính tuyệt đối
-- 		CASE WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL THEN neutrophils * wbc ELSE neutrophils_abs END AS neutrophils_abs,
-- 		neutrophils,
-- 		-- Bạch cầu lympho tuyệt đối
-- 		CASE WHEN lymphocytes_abs IS NULL AND lymphocytes IS NOT NULL THEN lymphocytes * wbc ELSE lymphocytes_abs END AS lymphocytes_abs,
-- 		lymphocytes,
-- 		-- NLR = Neutrophils_abs / Lymphocytes_abs
-- 		CASE WHEN lymphocytes_abs IS NULL OR lymphocytes_abs = 0 THEN NULL ELSE 
-- 				(CASE WHEN neutrophils_abs IS NULL AND neutrophils IS NOT NULL AND wbc IS NOT NULL 
-- 					THEN (neutrophils * wbc) / lymphocytes_abs
-- 					ELSE neutrophils_abs / lymphocytes_abs 
-- 				END)
-- 		END AS NLR
-- 	FROM lab_2

-- ),
first_labs AS (
  SELECT ie.stay_id,
         MAX(wbc) AS wbc, AVG(rbc) AS rbc, MAX(rdw) AS rdw,
         MAX(hemoglobin) AS hemoglobin, MIN(platelets) AS platelets,
         MAX(aniongap) AS aniongap, AVG(bicarbonate) AS bicarbonate,
         MAX(calcium) AS calcium, MAX(chloride) AS chloride,
         MAX(potassium) AS potassium, MAX(creatinine) AS creatinine,
		 MIN(glucose) AS glucose, 
		 MIN(triglycerides) AS triglycerides,
         MAX(alt) AS alt, MAX(alp) AS alp, MAX(ast) AS ast,
         MAX(pt) AS pt, MAX(ptt) AS ptt, MAX(inr) AS inr,
         MAX(neutrophils) AS neutrophils_max,
         MIN(neutrophils) AS neutrophils_min,
         MAX(albumin) AS albumin_max,
         MIN(albumin) AS albumin_min,
         MAX(bun) AS bun_max,
         MIN(bun) AS bun_min
  FROM ap_first_icu ie
  LEFT JOIN labs l USING (subject_id, hadm_id)
  LEFT JOIN lab_1 l1 USING (subject_id, hadm_id)
  LEFT JOIN lab_2 l2 USING (subject_id, hadm_id)
  WHERE l.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l1.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
     OR l2.charttime BETWEEN ie.intime - INTERVAL '6 hours' AND ie.intime + INTERVAL '1 days'
  GROUP BY ie.stay_id
  
),
diabetes_hypertension AS (
  SELECT DISTINCT subject_id, hadm_id,
         MAX(CASE WHEN icd_code LIKE '250%' OR icd_code LIKE 'E10%' OR icd_code LIKE 'E11%' THEN 1 ELSE 0 END) AS diabetes,
         MAX(CASE WHEN icd_code LIKE '401%' OR icd_code LIKE 'I10%' THEN 1 ELSE 0 END) AS hypertension
  FROM mimiciv_hosp.diagnoses_icd
  GROUP BY subject_id, hadm_id
),
sofa AS (
   --SOFA 24h 
   SELECT stay_id, sofa
  FROM mimiciv_derived.sofa
)
SELECT a.subject_id, a.hadm_id, a.stay_id, a.intime, a.dod, a.age, a.gender, a.race, a.los,
       dh.diabetes, dh.hypertension,
       f.wbc, f.rbc, f.rdw, f.hemoglobin, f.platelets,
       f.aniongap, f.bicarbonate, f.calcium, f.chloride, f.potassium,
       f.creatinine, f.glucose, f.triglycerides,
	   f.alt, f.alp, f.ast, f.pt, f.ptt, f.inr,
	   f.neutrophils_max, f.neutrophils_min,
	   f.albumin_max, f.albumin_min, f.bun_max, f.bun_min,
	   -- CASE WHEN s2.subject_id IS NOT NULL THEN 1 ELSE 0 END AS sepsis,
	   CASE WHEN s2.subject_id IS NOT NULL AND s.sofa >= 2 THEN 1 ELSE 0 END AS sepsis,
       f.neutrophils_max / NULLIF(f.albumin_max,0) AS NPAR,
       ROUND(LN((f.triglycerides * f.glucose) / 2.0)::numeric, 2) AS TyG,
       s.sofa,
       c.has_sepsis, c.aki, c.has_chronic_kidney_disease,
       t.has_crrt, t.has_mv, t.has_ercp, t.has_vasopressin,
       -- tử vong
	   EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	   EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.admittime AND a.dischtime THEN 1 ELSE 0 END AS hosp_mortality,
       CASE WHEN a.deathtime IS NOT NULL AND a.deathtime BETWEEN a.intime AND a.outtime THEN 1 ELSE 0 END AS icu_mortality,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '7 days'  THEN 1 ELSE 0 END AS mortality_7d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '28 days' THEN 1 ELSE 0 END AS mortality_28d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '90 days' THEN 1 ELSE 0 END AS mortality_90d,
       CASE WHEN a.dod IS NOT NULL AND a.dod <= a.intime + INTERVAL '365 days' THEN 1 ELSE 0 END AS mortality_1y
       
FROM ap_first_icu a
JOIN first_labs f ON a.stay_id = f.stay_id
JOIN comorbidity c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
JOIN treatments t  ON a.subject_id = t.subject_id AND a.hadm_id = t.hadm_id AND a.stay_id = t.stay_id
LEFT JOIN diabetes_hypertension dh ON a.subject_id = dh.subject_id AND a.hadm_id = dh.hadm_id
LEFT JOIN sofa s ON a.stay_id = s.stay_id
LEFT JOIN sepsis s2 ON a.subject_id = s2.subject_id AND a.hadm_id = s2.hadm_id
WHERE f.triglycerides IS NOT NULL
  AND f.glucose IS NOT NULL
  AND f.wbc IS NOT NULL
  AND f.hemoglobin IS NOT NULL
  AND f.platelets IS NOT NULL
  AND f.ALT IS NOT NULL
  AND f.AST IS NOT NULL
  AND f.pt IS NOT NULL
  AND f.ptt IS NOT NULL
  AND f.inr IS NOT NULL
  AND f.albumin_max IS NOT NULL
  AND f.albumin_max > 0;









