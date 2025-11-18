WITH apache AS(
    SELECT subject_id, stay_id,
        CASE
            WHEN temperature_mean < 30 THEN 4
            WHEN temperature_mean < 32 THEN 3
            WHEN temperature_mean < 34 THEN 2
            WHEN temperature_mean < 36 THEN 1
            WHEN temperature_mean < 38.5 THEN 0
            WHEN temperature_mean < 39 THEN 1
            WHEN temperature_mean < 41 THEN 3
            ELSE 4
        END AS temp_score,
        CASE
            WHEN mbp_mean < 50 THEN 4
            WHEN mbp_mean < 70 THEN 2
            WHEN mbp_mean < 110 THEN 0
            WHEN mbp_mean < 130 THEN 2
            WHEN mbp_mean < 160 THEN 3
            ELSE 4
        END AS mbp_score,
        CASE
            WHEN heart_rate_mean < 40 THEN 4
            WHEN heart_rate_mean < 55 THEN 3
            WHEN heart_rate_mean < 70 THEN 2
            WHEN heart_rate_mean < 110 THEN 0
            WHEN heart_rate_mean < 140 THEN 2
            WHEN heart_rate_mean < 180 THEN 3
            ELSE 4
        END AS hr_score,
        CASE
            WHEN resp_rate_mean < 6 THEN 4
            WHEN resp_rate_mean < 10 THEN 2
            WHEN resp_rate_mean < 12 THEN 1
            WHEN resp_rate_mean < 25 THEN 0
            WHEN resp_rate_mean < 35 THEN 1
            WHEN resp_rate_mean < 40 THEN 3
			ELSE 4
        END AS resp_score,
        CASE
            WHEN creatinine_max >= 3.5 THEN 4
            WHEN creatinine_max >= 2 THEN 3
            WHEN creatinine_max >= 1.5 THEN 2
            WHEN creatinine_max >= 0.6 THEN 0
            ELSE 2
        END AS creatinine_score,
        15 - gcs_min AS gcs_score,
        CASE
            WHEN ph_max >= 7.4 THEN 4
            WHEN ph_max >= 7.6 THEN 3
            WHEN ph_max >= 7.5 THEN 1
            WHEN ph_max >= 7.33 THEN 0
            WHEN ph_max >= 7.25 THEN 2
            WHEN ph_max >= 7.15 THEN 3
            ELSE 4
        END AS ph_score,
        CASE
            WHEN pao2fio2ratio_max > 70 THEN 0
            WHEN pao2fio2ratio_max > 60 THEN 1
            WHEN pao2fio2ratio_max > 55 THEN 3
            ELSE 4
        END AS fio2_score,
		CASE
			WHEN first_day_lab.sodium_max >= 180 THEN 4
            WHEN first_day_lab.sodium_max >= 160 THEN 3
            WHEN first_day_lab.sodium_max >= 155 THEN 2
            WHEN first_day_lab.sodium_max >= 150 THEN 1
            WHEN first_day_lab.sodium_max >= 130 THEN 0
            WHEN first_day_lab.sodium_max >= 120 THEN 2
            WHEN first_day_lab.sodium_max >= 111 THEN 3
            ELSE 4
		END AS sodium_score,
		CASE
            WHEN first_day_lab.potassium_max >= 7 THEN 4
            WHEN first_day_lab.potassium_max >= 6 THEN 3
            WHEN first_day_lab.potassium_max >= 5.5 THEN 1
            WHEN first_day_lab.potassium_max >= 3.5 THEN 0
            WHEN first_day_lab.potassium_max >= 3 THEN 1
            WHEN first_day_lab.potassium_max >= 2.5 THEN 2
            ELSE 4
        END AS potassium_score,
		CASE
            WHEN first_day_lab.hematocrit_max < 20 THEN 4
            WHEN first_day_lab.hematocrit_max < 30 THEN 2
            WHEN first_day_lab.hematocrit_max < 46 THEN 0
            WHEN first_day_lab.hematocrit_max < 50 THEN 1
            WHEN first_day_lab.hematocrit_max < 60 THEN 2
            ELSE 4
        END AS hematocrit_score,
		CASE
            WHEN wbc_max >= 40 THEN 4
            WHEN wbc_max >= 20 THEN 2
            WHEN wbc_max >= 15 THEN 1
            WHEN wbc_max >= 3 THEN 0
            WHEN wbc_max >= 1 THEN 2
            ELSE 4
        END AS wbc_score
		-- CASE
  --           WHEN age >= 75 THEN 6
  --           WHEN age >= 65 THEN 5
  --           WHEN age >= 55 THEN 3
  --           WHEN age >= 45 THEN 2
  --           ELSE 0
  --       END AS age_score,
		
    FROM mimiciv_derived.first_day_vitalsign
    JOIN mimiciv_derived.first_day_lab USING(subject_id, stay_id)
    JOIN mimiciv_derived.first_day_gcs USING(subject_id, stay_id)
    JOIN mimiciv_derived.first_day_bg USING(subject_id, stay_id)
)SELECT subject_id, stay_id,
    temp_score + mbp_score + hr_score + resp_score + creatinine_score + gcs_score + ph_score + fio2_score +
	sodium_score + potassium_score + hematocrit_score + wbc_score AS apache_score
FROM apache;


-- SELECT * FROM mimiciv_derived.first_day_vitalsign LIMIT 100;
-- SELECT * FROM mimiciv_derived.first_day_lab LIMIT 100;
-- SELECT * FROM mimiciv_derived.first_day_gcs LIMIT 100;
-- SELECT * FROM mimiciv_derived.first_day_bg LIMIT 100;
