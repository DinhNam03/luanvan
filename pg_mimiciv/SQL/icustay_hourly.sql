CREATE TABLE mimiciv_derived.icustay_hourly AS
with all_hours as
(
select
  it.stay_id

  -- ceiling the intime to the nearest hour by adding 59 minutes then truncating
  -- note thart we truncate by parsing as string, rather than using DATETIME_TRUNC
  -- this is done to enable compatibility with psql
  , date_trunc('hour', it.intime_hr + interval '59 minutes') AS endtime

  -- create integers for each charttime in hours from admission
  -- so 0 is admission time, 1 is one hour after admission, etc, up to ICU disch
  --  we allow 24 hours before ICU admission (to grab labs before admit)
  , generate_series(
  -24,
  CEIL(EXTRACT(EPOCH FROM (it.outtime_hr - it.intime_hr)) / 3600)::int,
  1
) AS hr


  from mimiciv_derived.icustay_times it
)
SELECT stay_id
, CAST(hr AS integer) AS hr,
endtime + (CAST(hr AS integer) * interval '1 hour') AS endtime


FROM all_hours
