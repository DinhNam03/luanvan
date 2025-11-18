CREATE TABLE mimiciv_derived.first_day_urine_output AS
SELECT
  -- patient identifiers
  ie.subject_id
  , ie.stay_id
  , SUM(urineoutput) AS urineoutput
FROM mimiciv_icu.icustays ie
LEFT JOIN mimiciv_derived.urine_output uo
    ON ie.stay_id = uo.stay_id
    AND uo.charttime >= ie.intime
    AND uo.charttime <= ie.intime + INTERVAL '1 day'
GROUP BY ie.subject_id, ie.stay_id;
