CREATE TABLE mimiciv_derived.ventilation AS
WITH tm AS
(
  SELECT stay_id, charttime
  FROM mimiciv_derived.ventilator_setting
  UNION DISTINCT
  SELECT stay_id, charttime
  FROM mimiciv_derived.oxygen_delivery
)
, vs AS
(
    SELECT tm.stay_id, tm.charttime
    , o2_delivery_device_1
    , COALESCE(ventilator_mode, ventilator_mode_hamilton) AS vent_mode

    , CASE
        WHEN o2_delivery_device_1 IN ('Tracheostomy tube')
            THEN 'Trach'

        WHEN o2_delivery_device_1 IN ('Endotracheal tube')
          OR COALESCE(ventilator_mode, ventilator_mode_hamilton) IN (
            '(S) CMV','APRV','APRV/Biphasic+ApnPress','APRV/Biphasic+ApnVol',
            'APV (cmv)','Ambient','Apnea Ventilation','CMV','CMV/ASSIST',
            'CMV/ASSIST/AutoFlow','CMV/AutoFlow','CPAP/PPS','CPAP/PSV+Apn TCPL',
            'CPAP/PSV+ApnPres','CPAP/PSV+ApnVol','MMV','MMV/AutoFlow','MMV/PSV',
            'MMV/PSV/AutoFlow','P-CMV','PCV+','PCV+/PSV','PCV+Assist',
            'PRES/AC','PRVC/AC','PRVC/SIMV','PSV/SBT','SIMV','SIMV/AutoFlow',
            'SIMV/PRES','SIMV/PSV','SIMV/PSV/AutoFlow','SIMV/VOL',
            'SYNCHRON MASTER','SYNCHRON SLAVE','VOL/AC'
          )
          OR ventilator_mode_hamilton IN (
            'APRV','APV (cmv)','Ambient','(S) CMV','P-CMV',
            'SIMV','APV (simv)','P-SIMV','VS','ASV'
          )
            THEN 'InvasiveVent'

        WHEN o2_delivery_device_1 IN ('Bipap mask ', 'CPAP mask ')
          OR ventilator_mode_hamilton IN ('DuoPaP','NIV','NIV-ST')
            THEN 'NonInvasiveVent'

        WHEN o2_delivery_device_1 IN ('High flow neb','High flow nasal cannula')
            THEN 'HighFlow'

        WHEN o2_delivery_device_1 in (
            'Nasal cannula','Face tent','Aerosol-cool','Non-rebreather',
            'Venti mask ','Medium conc mask ','T-piece','Ultrasonic neb',
            'Vapomist','Oxymizer'
        )
            THEN 'Oxygen'
        ELSE NULL
      END AS ventilation_status

  FROM tm
  LEFT JOIN mimiciv_derived.ventilator_setting vs
      ON tm.stay_id = vs.stay_id
      AND tm.charttime = vs.charttime
  LEFT JOIN mimiciv_derived.oxygen_delivery od
      ON tm.stay_id = od.stay_id
      AND tm.charttime = od.charttime
)
, vd0 AS
(
    SELECT
      stay_id, charttime
      , o2_delivery_device_1
      , vent_mode
      , LAG(charttime, 1) OVER (PARTITION BY stay_id, ventilation_status ORDER BY charttime) AS charttime_lag
      , LEAD(charttime, 1) OVER w AS charttime_lead
      , ventilation_status
      , LAG(ventilation_status, 1) OVER w AS ventilation_status_lag
    FROM vs
    WHERE ventilation_status IS NOT NULL
    WINDOW w AS (PARTITION BY stay_id ORDER BY charttime)
)
, vd1 as
(
    SELECT
        stay_id
        , o2_delivery_device_1
        , vent_mode
        , charttime_lag
        , charttime
        , charttime_lead
        , ventilation_status

        , EXTRACT(EPOCH FROM (charttime - charttime_lag)) / 3600.0 as ventduration

        , CASE
            WHEN EXTRACT(EPOCH FROM (charttime - charttime_lag)) / 3600.0 >= 14 THEN 1
            WHEN ventilation_status_lag IS NULL THEN 1
            WHEN ventilation_status_lag != ventilation_status THEN 1
            ELSE 0
          END AS new_status
    FROM vd0
)
, vd2 as
(
    SELECT vd1.*
    , SUM(new_status) OVER (PARTITION BY stay_id ORDER BY charttime) AS vent_num
    FROM vd1
)
SELECT stay_id
  , MIN(charttime) AS starttime
  , MAX(
        CASE
            WHEN charttime_lead IS NULL
              OR EXTRACT(EPOCH FROM (charttime_lead - charttime)) / 3600.0 >= 14
                THEN charttime
            ELSE charttime_lead
        END
   ) AS endtime
  , MAX(ventilation_status) AS ventilation_status
FROM vd2
GROUP BY stay_id, vent_num
HAVING MIN(charttime) != MAX(charttime)
;
