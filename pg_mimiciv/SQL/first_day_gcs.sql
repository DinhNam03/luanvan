CREATE TABLE mimiciv_derived.first_day_gcs AS
WITH gcs_final AS
(
    SELECT
        gcs.*
        -- This sorts the data by GCS
        -- rn = 1 is the the lowest total GCS value
        , ROW_NUMBER () OVER
        (
            PARTITION BY gcs.stay_id
            ORDER BY gcs.GCS
        ) as gcs_seq
    FROM mimiciv_derived.gcs gcs
)
SELECT
    ie.subject_id
    , ie.stay_id
    -- The minimum GCS is determined by the above row partition
    -- we only join if gcs_seq = 1
    , gcs AS gcs_min
    , gcs_motor
    , gcs_verbal
    , gcs_eyes
    , gcs_unable
FROM mimiciv_icu.icustays ie
LEFT JOIN gcs_final gs
    ON ie.stay_id = gs.stay_id
    AND gs.gcs_seq = 1
;