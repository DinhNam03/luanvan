SELECT * FROM mimiciv_hosp.d_icd_diagnoses WHERE long_title LIKE '%Kali%'
SELECT * FROM mimiciv_hosp.diagnoses_icd

SELECT * FROM mimiciv_hosp.d_icd_diagnoses WHERE icd_code IN('5712', '5715', '5716', 'K703', 'K717', 'K743', 'K744', 'K745', 'K746');
SELECT * FROM mimiciv_hosp.diagnoses_icd WHERE icd_code IN('5712', '5715', '5716', 'K703', 'K717', 'K743', 'K744', 'K745', 'K746');


WITH ap AS(
  --7.481
  SELECT distinct subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
)
, ap_icu AS(
  --74.829
  SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
    EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
    race,
    ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
 	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	WHERE los >= 1 OR los IS NULL
), ap_first_icu AS(
  	SELECT * FROM ap_icu WHERE rn = 1
), sepsis AS(
    SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
    	WHERE icd_code IN('5712', '5715', '5716', 'K703', 'K717', 'K743', 'K744', 'K745', 'K746')
), aki AS (
	SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code 
		IN ('5846', '5847', '5848', '5849', 'N17', 'N170', 'N171', 'N172', 'N178', 'N179')
), chronic_kidney_disease AS(
    SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
    WHERE icd_code IN('5851', '5852', '5853', '5854', '5855', '5856', '5859', 'N18', 'N181', 'N182', 'N183', 'N1830', 'N1831', 'N1832', 'N184', 'N185', 'N186', 'N189')
), comorbidity AS(
    SELECT a.subject_id, a.hadm_id, a.stay_id,
        CASE WHEN d.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
        CASE WHEN d.subject_id IS NULL THEN 0 ELSE 1 END AS has_aki,
        CASE WHEN ckd.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease
    FROM ap_first_icu AS a
        LEFT JOIN sepsis d ON d.subject_id = a.subject_id AND d.hadm_id = a.hadm_id
		LEFT JOIN aki ON aki.subject_id = a.subject_id AND aki.hadm_id = a.hadm_id
        LEFT JOIN chronic_kidney_disease AS ckd ON ckd.subject_id = a.subject_id AND ckd.hadm_id = a.hadm_id
		
) SELECT * FROM comorbidity;




--CRRT

-- SELECT * FROM mimiciv_icu.d_items WHERE itemid IN(227290, 230177, 225956);


WITH ap AS(
  --7.481
  SELECT distinct subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
)
, ap_icu AS(
  --74.829
  SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
    EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
    race,
    ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
 	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	WHERE los >= 1 OR los IS NULL
), ap_first_icu AS(
  	SELECT * FROM ap_icu WHERE rn = 1
), crrt AS (
	SELECT DISTINCT subject_id, hadm_id, stay_id
		FROM mimiciv_icu.chartevents WHERE itemid IN(227290, 230177, 225956)
), mv AS (
	SELECT DISTINCT subject_id, hadm_id
		FROM mimiciv_hosp.procedures_icd WHERE icd_code IN ('9670','9671','9672', '5A1935Z', '5A1945Z', '5A1955Z')
), ercp AS(
	SELECT DISTINCT subject_id, hadm_id
		FROM mimiciv_hosp.procedures_icd WHERE icd_code IN ('5110', '5213')
), vasopressin AS(
	SELECT DISTINCT subject_id, hadm_id, stay_id
		FROM mimiciv_icu.inputevents WHERE itemid = 222315
)
, treatments AS(
	SELECT a.subject_id, a.hadm_id, a.stay_id,
		CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
		CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
		CASE WHEN ercp.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
		CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
		FROM ap_first_icu AS a
	LEFT JOIN crrt AS c ON a.subject_id = c.subject_id AND a.hadm_id = c.hadm_id AND a.stay_id = c.stay_id
	LEFT JOIN mv ON a.subject_id = mv.subject_id AND a.hadm_id = mv.hadm_id
	LEFT JOIN ercp ON a.subject_id = ercp.subject_id AND a.hadm_id = ercp.hadm_id
	LEFT JOIN vasopressin AS v ON a.subject_id = v.subject_id AND a.hadm_id = v.hadm_id AND a.stay_id = v.stay_id
		
)SELECT * FROM treatments



--MV - Mechanical Ventilation
SELECT DISTINCT subject_id, hadm_id
FROM mimiciv_hosp.procedures_icd WHERE icd_code IN ('9670','9671','9672', '5A1935Z', '5A1945Z', '5A1955Z');

--Vasopressin
SELECT * FROM mimiciv_icu.d_items WHERE itemid = 222315;

--inputevents
SELECT DISTINCT subject_id, hadm_id, stay_id
FROM mimiciv_icu.inputevents WHERE itemid = 222315;

--C-reactive protein (CRP)
SELECT * FROM mimiciv_hosp.d_labitems WHERE itemid IN(50889, 51652);

SELECT DISTINCT subject_id, hadm_id, speciment_id, charttime
FROM mimiciv_hosp.labevents WHERE itemid IN(50889, 51652);





--AKI
SELECT * FROM mimiciv_hosp.d_icd_diagnoses WHERE icd_code LIKE '584%' OR icd_code LIKE 'N17%';
SELECT * FROM mimiciv_hosp.d_icd_diagnoses WHERE icd_code IN ('5846', '5847', '5848', '5849', 'N17', 'N170', 'N171', 'N172', 'N178', 'N179')


WITH ap AS(
  --7.481
  SELECT distinct subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
)
, ap_icu AS(
  --74.829
  SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
    EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
    race,
    ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY intime) AS rn
  	FROM mimiciv_icu.icustays
 	JOIN mimiciv_hosp.patients USING (subject_id)
  	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  	JOIN ap USING (subject_id, hadm_id)
  	WHERE los >= 1 OR los IS NULL
), ap_first_icu AS(
  	SELECT * FROM ap_icu WHERE rn = 1
), aki AS (
	SELECT * FROM mimiciv_hosp.diagnoses_icd WHERE icd_code 
		IN ('5846', '5847', '5848', '5849', 'N17', 'N170', 'N171', 'N172', 'N178', 'N179')
), aki_first_icu AS	(
	SELECT a.subject_id, a.hadm_id, a.stay_id,
        CASE WHEN aki.subject_id IS NULL THEN 0 ELSE 1 END AS has_aki
    	FROM aki RIGHT JOIN ap_first_icu AS a ON aki.subject_id = a.subject_id AND aki.hadm_id = a.hadm_id
)SELECT * FROM aki_first_icu;


--Chronic kidney disease - Bệnh thận mãn tính



-- WITH ap AS(
--   --7.481
--   SELECT distinct subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
-- )
-- , ap_icu AS(
--   --74.829
--   SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
--     EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
--     race,
--     ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY intime) AS rn
--   	FROM mimiciv_icu.icustays
--  	JOIN mimiciv_hosp.patients USING (subject_id)
--   	JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
--   	JOIN ap USING (subject_id, hadm_id)
--   	WHERE los >= 1 OR los IS NULL
-- ), ap_first_icu AS(
--   	SELECT * FROM ap_icu WHERE rn = 1
-- ) 
-- --SELECT * FROM ap_first_icu, 
-- , crp AS(
-- 	SELECT DISTINCT subject_id, hadm_id, speciment_id, charttime
-- 		FROM mimiciv_hosp.labevents WHERE itemid IN(50889, 51652)
-- ), test AS(
-- 	SELECT a.subject_id, a.hadm_id, a.speciment_id, a.charttime
-- 		CASE WHEN crp.subject_id IS NULL THEN 0 ELSE 1 END AS has_crp
-- 		FROM ap_first_icu AS a
-- 	LEFT JOIN crp ON a.subject_id = crp.subject_id AND a.hadm_id = crp.hadm_id AND a.speciment_id = crp.speciment_id AND a.charttime = crp.charttime
	
		
-- )SELECT * FROM test;


--gộp 
WITH ap AS(
  --7.481 bệnh nhân viêm tụy cấp
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
), ap_icu AS(
  --74.829 lần nhập ICU
  SELECT subject_id, hadm_id, stay_id, intime, outtime, los, dod, gender, deathtime, admittime, dischtime,
         EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
         race,
         ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY intime) AS rn
  FROM mimiciv_icu.icustays
  JOIN mimiciv_hosp.patients USING (subject_id)
  JOIN mimiciv_hosp.admissions USING (subject_id, hadm_id)
  JOIN ap USING (subject_id, hadm_id)
  WHERE los >= 1 OR los IS NULL
)
, ap_first_icu AS(
  SELECT * FROM ap_icu WHERE rn = 1
)
-- Các bệnh kèm
, sepsis AS(
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5712', '5715', '5716', 'K703', 'K717', 'K743', 'K744', 'K745', 'K746')
)
, aki AS(
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd 
  WHERE icd_code IN ('5846', '5847', '5848', '5849', 'N17', 'N170', 'N171', 'N172', 'N178', 'N179')
)
, chronic_kidney_disease AS(
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.diagnoses_icd
  WHERE icd_code IN ('5851', '5852', '5853', '5854', '5855', '5856', '5859',
                     'N18', 'N181', 'N182', 'N183', 'N1830', 'N1831', 'N1832',
                     'N184', 'N185', 'N186', 'N189')
)
-- Các phương pháp điều trị
, crrt AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.chartevents 
  WHERE itemid IN (227290, 230177, 225956)
)
, mv AS (
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd 
  WHERE icd_code IN ('9670','9671','9672', '5A1935Z', '5A1945Z', '5A1955Z')
)
, ercp AS(
  SELECT DISTINCT subject_id, hadm_id FROM mimiciv_hosp.procedures_icd 
  WHERE icd_code IN ('5110', '5213')
)
, vasopressin AS(
  SELECT DISTINCT subject_id, hadm_id, stay_id FROM mimiciv_icu.inputevents 
  WHERE itemid = 222315
)
-- Gộp chung
, final AS(
  SELECT a.subject_id, a.hadm_id, a.stay_id,
         a.intime, a.outtime, a.los, a.age, a.gender, a.race, a.admittime, a.dischtime, a.dod, a.deathtime,
         -- bệnh đi kèm
         CASE WHEN s.subject_id IS NULL THEN 0 ELSE 1 END AS has_sepsis,
         CASE WHEN aki.subject_id IS NULL THEN 0 ELSE 1 END AS has_aki,
         CASE WHEN ckd.subject_id IS NULL THEN 0 ELSE 1 END AS has_chronic_kidney_disease,
         -- điều trị
         CASE WHEN c.subject_id IS NULL THEN 0 ELSE 1 END AS has_crrt,
         CASE WHEN mv.subject_id IS NULL THEN 0 ELSE 1 END AS has_mv,
         CASE WHEN ercp.subject_id IS NULL THEN 0 ELSE 1 END AS has_ercp,
         CASE WHEN v.subject_id IS NULL THEN 0 ELSE 1 END AS has_vasopressin
  FROM ap_first_icu AS a
  LEFT JOIN sepsis AS s ON s.subject_id = a.subject_id AND s.hadm_id = a.hadm_id
  LEFT JOIN aki ON aki.subject_id = a.subject_id AND aki.hadm_id = a.hadm_id
  LEFT JOIN chronic_kidney_disease ckd ON ckd.subject_id = a.subject_id AND ckd.hadm_id = a.hadm_id
  LEFT JOIN crrt AS c ON c.subject_id = a.subject_id AND c.hadm_id = a.hadm_id AND c.stay_id = a.stay_id
  LEFT JOIN mv ON mv.subject_id = a.subject_id AND mv.hadm_id = a.hadm_id
  LEFT JOIN ercp ON ercp.subject_id = a.subject_id AND ercp.hadm_id = a.hadm_id
  LEFT JOIN vasopressin AS v ON v.subject_id = a.subject_id AND v.hadm_id = a.hadm_id AND v.stay_id = a.stay_id
)
SELECT * FROM final;



