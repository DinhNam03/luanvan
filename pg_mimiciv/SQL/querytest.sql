WITH base AS(
	SELECT subject_id, stay_id, charttime,
		MAX(CASE
			WHEN itemid = '223900' AND value = 'No Response-ETT' THEN 0
			WHEN itemid = '223900' THEN valuenum ELSE NULL
		END) AS GCSVerbal,
		MAX(CASE WHEN itemid = '223901' THEN valuenum ELSE NULL END) AS GCSMotor,
		MAX(CASE WHEN itemid = '220739' THEN valuenum ELSE NULL END) AS GCSEyes,
		ROW_NUMBER() OVER(PARTITION BY stay_id ORDER BY charttime) AS rn
		FROM mimiciv_icu.chartevents
		WHERE itemid IN (223900, 223901, 220739)
		GROUP BY subject_id, stay_id, charttime --2217787
)SELECT b.*, b2.GCSMotor AS GCSMotorPrev, b2.GCSEyes AS GCSEyesPrev, b2.GCSVerbal AS GCSVerbalPrev
	, case
	      -- replace GCS during sedation with 15
	      when b.GCSVerbal = 0
	        then 15
	      when b.GCSVerbal is null and b2.GCSVerbal = 0
	        then 15
	      -- if previously they were intub, but they aren't now, do not use previous GCS values
	      when b2.GCSVerbal = 0
	        then
	            coalesce(b.GCSMotor,6)
	          + coalesce(b.GCSVerbal,5)
	          + coalesce(b.GCSEyes,4)
	      -- otherwise, add up score normally, imputing previous value if none available at current time
	      else
	            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
	          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
	          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
	      end as GCS
		FROM base AS b LEFT JOIN base AS b2 ON b.stay_id = b2.stay_id AND b.rn = b2.rn + 1 AND b.charttime > b2.charttime + INTERVAL '6 HOUR';


		