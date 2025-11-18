
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
), labs AS(
  SELECT
    subject_id, hadm_id, charttime,
    MAX(CASE WHEN itemid = 51256 THEN valuenum ELSE NULL END) AS neutrophils,
    MAX(CASE WHEN itemid = 50862 THEN valuenum ELSE NULL END) albumin,
    MAX(CASE WHEN itemid = 51006 THEN valuenum ELSE NULL END) bun
  	FROM mimiciv_hosp.labevents WHERE hadm_id IS NOT NULL AND itemid IN (50862, 51006, 51256) AND valuenum IS NOT NULL
  	GROUP BY subject_id, hadm_id, charttime, specimen_id
), first_labs AS(
  SELECT stay_id,
  	MAX(neutrophils) neutrophils_max,
    MIN(neutrophils) neutrophils_min,
    MAX(albumin) albumin_max,
    MIN(albumin) albumin_min,
    MAX(bun) bun_max,
    MIN(bun) bun_min
  FROM labs
  JOIN ap_first_icu ie USING(subject_id, hadm_id)
  WHERE charttime >= ie.intime - INTERVAL '6 hours'
  AND charttime <= ie.intime + INTERVAL '4 day'
  GROUP BY stay_id
)SELECT subject_id, hadm_id, stay_id, gender, race, age,
  neutrophils_max,
  albumin_max, bun_max,
  neutrophils_max / albumin_max AS NPAR,
  los,
  CASE WHEN deathtime IS NOT NULL AND deathtime BETWEEN admittime AND dischtime THEN 1 ELSE 0 END hosp_mortality,
  CASE WHEN deathtime IS NOT NULL AND deathtime BETWEEN intime AND outtime THEN 1 ELSE 0 END icu_mortality,
  CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '30 DAYS' THEN 1 ELSE 0 END mortality_30d,
  CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '90 DAYS' THEN 1 ELSE 0 END mortality_90d,
  CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '365 DAYS' THEN 1 ELSE 0 END mortality_1y
FROM ap_first_icu
JOIN first_labs USING(stay_id)
WHERE neutrophils_max IS NOT NULL AND albumin_max IS NOT NULL AND albumin_max > 0;
















SELECT * FROM mimiciv_hosp.patients
SELECT * FROM mimiciv_hosp.d_labitems
SELECT * FROM mimiciv_hosp.admissions
SELECT * FROM mimiciv_hosp.d_icd_diagnoses
SELECT * FROM mimiciv_hosp.diagnoses_icd
SELECT * FROM mimiciv_icu.icustays limit 100
SELECT * FROM mimiciv_icu.chartevents limit 100


-- DELETE FROM mimiciv_hosp.patients
-- DELETE FROM mimiciv_hosp.d_labitems
-- DELETE FROM mimiciv_hosp.admissions
-- DELETE FROM mimiciv_hosp.d_icd_diagnoses
-- DELETE FROM mimiciv_hosp.diagnoses_icd
-- DELETE FROM mimiciv_icu.icustays
-- DELETE FROM mimiciv_icu.chartevents





SELECT * FROM mimiciv_hosp.d_labitems limit 10;

SELECT * FROM mimiciv_hosp.d_icd_diagnoses limit 10;

--Bảng tra loại bệnh
SELECT * FROM mimiciv_hosp.d_icd_diagnoses WHERE icd_code = '5770' OR icd_code LIKE 'K85%';

--546,028
SELECT COUNT(*) FROM mimiciv_hosp.admissions;

--94,458
SELECT COUNT(*) FROM mimiciv_icu.icustays;

--host: subject_id, hadm_id


--Thông tin các bệnh nhân viêm tuỵ
WITH ap AS(
	-- 7481
	SELECT DISTINCT subject_id, hadm_id
	FROM mimiciv_hosp.diagnoses_icd
	WHERE icd_code = '5770' OR icd_code LIKE 'K85%'
)
--1686 -> 1405
, ap_icu_stay as(
SELECT *, 
	EXTRACT(YEAR FROM admittime) - anchor_year + anchor_age AS age,
	ROW_NUMBER() OVER(PARTITION BY i.subject_id ORDER BY intime) as rn
	FROM mimiciv_icu.icustays as i
	JOIN mimiciv_hosp.patients AS p USING(subject_id)
	JOIN mimiciv_hosp.admissions AS a USING (subject_id, hadm_id)
	-- JOIN ap ON i.subject_id = ap.subject_id AND i.hadm_id = ap.hadm_id
	-- Natural join
	JOIN ap USING (subject_id, hadm_id)
	WHERE los >= 1 -- thời gian nằm hồi sức
), ap_first_icu_stay AS(
	SELECT * FROM ap_icu_stay WHERE rn = 1
)
SELECT intime, outtime, deathtime, dod, admittime, dischtime, 
	heart_rate_mean, systolic_blood_pressure_mean, temperature_body_mean, resp_rate_mean, 
	EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu,
	EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp,
	CASE WHEN deathtime IS NOT NULL AND deathtime BETWEEN admittime AND dischtime THEN 1 ELSE 0 END hosp_mortality,
	CASE WHEN deathtime IS NOT NULL AND deathtime BETWEEN intime AND outtime THEN 1 ELSE 0 END icu_mortality,

	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '7 DAYS' THEN 1 ELSE 0 END mortality_7d,
	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '14 DAYS' THEN 1 ELSE 0 END mortality_14d,
	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '21 DAYS' THEN 1 ELSE 0 END mortality_21d,
	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '28 DAYS' THEN 1 ELSE 0 END mortality_28d,
	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '90 DAYS' THEN 1 ELSE 0 END mortality_90d,
	CASE WHEN dod IS NOT NULL AND dod <= intime + INTERVAL '365 DAYS' THEN 1 ELSE 0 END mortality_365d
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 7 THEN 1 ELSE 0 END mortality_7d,
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 14 THEN 1 ELSE 0 END mortality_14d,
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 21 THEN 1 ELSE 0 END mortality_21d,
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 28 THEN 1 ELSE 0 END mortality_28d,
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 90 THEN 1 ELSE 0 END mortality_90d,
	-- CASE WHEN deathtime IS NOT NULL AND EXTRACT(EPOCH FROM (outtime - intime)) / 86400 <= 365 THEN 1 ELSE 0 END mortality_1y
	
FROM ap_first_icu_stay -- 1100 -- bệnh nhân viêm tụy nằm trong icu lần 1, thời gian nằm > 24h 
LEFT JOIN mimiciv_derived.first_vital_sign USING (subject_id, hadm_id, stay_id)

----
CREATE TABLE mimiciv_derived.first_vital_sign AS
WITH vital_sign AS(
	SELECT subject_id, hadm_id, stay_id, charttime,
	AVG(CASE WHEN itemid = 220045 AND valuenum > 0 AND valuenum < 300 THEN valuenum ELSE null END) AS heart_rate,
	AVG(CASE WHEN itemid IN (220179, 220050, 225309) AND valuenum > 0 AND valuenum < 400 THEN valuenum ELSE null END) AS systolic_blood_pressure,
	AVG(
		CASE
			WHEN itemid = 223761 AND valuenum > 70 AND valuenum < 120 THEN (valuenum - 30) / 1.8
			WHEN itemid = 223762 AND valuenum > 10 AND valuenum < 50 THEN valuenum
			ELSE NULL
		END
	)AS temperature_body,
	AVG(CASE WHEN itemid IN (220180, 220051, 225310)
                AND valuenum > 0
                AND valuenum < 300
                THEN valuenum END
    ) AS diastolic_blood_pressure,
	AVG(CASE WHEN itemid IN (220052, 220181, 225312)
                AND valuenum > 0
                AND valuenum < 300
                THEN valuenum END
    ) AS mean_arterial_pressure,
	AVG(CASE WHEN itemid IN (220210, 224690)
                AND valuenum > 0
                AND valuenum < 70
                THEN valuenum END
    ) AS resp_rate,
	AVG(CASE WHEN itemid IN (220277)
                AND valuenum > 0
                AND valuenum <= 100
                THEN valuenum END
    ) AS oxygen_saturation
	FROM mimiciv_icu.chartevents WHERE itemid IN (220045, 220179, 220050, 225309, 223761, 223762, 220180, 220051, 225310, 220052, 220181, 225312, 220210, 224690, 220277)
	GROUP BY subject_id, hadm_id, stay_id, charttime
), first_vital_sign AS(
	SELECT subject_id, hadm_id, stay_id,
			AVG(heart_rate) AS heart_rate_mean,
			MIN(heart_rate) AS heart_rate_min,
			MAX(heart_rate) AS heart_rate_max,
			AVG(systolic_blood_pressure) AS systolic_blood_pressure_mean,
			MIN(systolic_blood_pressure) AS systolic_blood_pressure_min,
			MAX(systolic_blood_pressure) AS systolic_blood_pressure_max,
			AVG(temperature_body) AS temperature_body_mean,
			MIN(temperature_body) AS temperature_body_min,
			MAX(temperature_body) AS temperature_body_max,
			MIN(diastolic_blood_pressure) AS diastolic_blood_pressure_min,
			MAX(diastolic_blood_pressure) AS diastolic_blood_pressure_max,
			AVG(diastolic_blood_pressure) AS diastolic_blood_pressure_mean,
			MIN(mean_arterial_pressure) AS mean_arterial_pressure_min,
			MAX(mean_arterial_pressure) AS mean_arterial_pressure_max,
			AVG(mean_arterial_pressure) AS mean_arterial_pressure_mean,
			MIN(resp_rate) AS resp_rate_min,
			MAX(resp_rate) AS resp_rate_max,
			AVG(resp_rate) AS resp_rate_mean,
			MIN(oxygen_saturation) AS oxygen_saturation_min,
			MAX(oxygen_saturation) AS oxygen_saturation_max,
			AVG(oxygen_saturation) AS oxygen_saturation_mean
	FROM vital_sign JOIN mimiciv_icu.icustays USING (subject_id, hadm_id, stay_id)
		WHERE charttime BETWEEN intime - INTERVAL '6 HOURS' AND intime + INTERVAL '24 HOURS'
		GROUP BY subject_id, hadm_id, stay_id
) SELECT * FROM first_vital_sign;






-------------------

SELECT *, EXTRACT(EPOCH FROM (outtime - intime)) / 86400 AS los_icu
FROM mimiciv_icu.icustays LIMIT 10;

SELECT *, EXTRACT(EPOCH FROM (dischtime - admittime)) / 86400 AS los_hosp
FROM mimiciv_hosp.admissions
LIMIT 10;

SELECT * FROM mimiciv_icu.icustays;
SELECT * FROM mimiciv_hosp.patients LIMIT 10;

SELECT 3600 * 24;


--heart_rate
--Outlier
SELECT MIN(valuenum), MAX(valuenum), AVG(valuenum)
FROM mimiciv_icu.chartevents WHERE itemid IN (220045);


SELECT subject_id, hadm_id, stay_id, charttime,
	AVG(CASE WHEN itemid = 8752069 AND valuenum > 0 AND valuenum < 300 THEN valuenum ELSE null END) AS heart_rate,
	AVG(CASE WHEN itemid IN (220179, 220050, 225309) AND valuenum > 0 AND valuenum < 400 THEN valuenum ELSE null END) AS systolic_blood_pressure,
	AVG(
		CASE
			WHEN itemid = 223761 AND valuenum > 70 AND valuenum < 120 THEN (valuenum - 30) /1.8
			WHEN itemid = 223762 AND valuenum > 10 AND valuenum < 50 THEN valuenum
			ELSE NULL
		END
	) AS temperature_body
FROM mimiciv_icu.chartevents WHERE itemid IN (220045, 220179, 220050, 225309, 223762, 223762)
GROUP BY subject_id, hadm_id, stay_id, charttime;

--nhiet do
SELECT MIN(valuenum), MAX(valuenum), AVG(valuenum)
	FROM mimiciv_icu.chartevents WHERE itemid IN (223762);



--degC = (degF - 30)/1.8



SELECT * FROM mimiciv_icu.chartevents WHERE itemid IN (223762, 223761);

SELECT * FROM mimiciv_icu.d_items WHERE itemid IN (223762, 223761);

SELECT * FROM mimiciv_icu.d_items WHERE LOWER(label) LIKE '%temperature%';





SELECT * FROM mimiciv.icu.d_item WHERE itemid IN(220179, 220050, 225309);

SELECT * FROM mimiciv_icu.chartevents 
WHERE itemid IN (220045) AND valuenum > 0 AND valuenum < 300;

SELECT * FROM mimiciv_icu.d_items 
WHERE itemid IN (220045);

SELECT * FROM mimiciv_icu.d_items WHERE LOWER(label) LIKE '%heart rate%';


--Glucose









--icu: subject_id, hadm_id, stay_id

--Thông tin người bệnh nhân
--subject_id (key)
SELECT * FROM mimiciv_hosp.patients;

--subject_id, hadm_id (key)
SELECT * FROM mimiciv_hosp.admissions;

--subject_id, hadm_id, stay_id (key)
SELECT * FROM mimiciv_icu.icustays
limit 100;

--hosp
   --patients => subject_id (key)
   --admissions => subject_id, hadm_id
      --> subject_id, hadm_id

--icu => subject_id, hadm_id, stay_id




SELECT *  FROM mimiciv_hosp.labevents WHERE itemid = 51274 AND hadm_id IS NOT NULL



