CREATE TABLE mimiciv_derived.dobutamine AS
select
stay_id, linkorderid
, rate as vaso_rate
, amount as vaso_amount
, starttime
, endtime
from mimiciv_icu.inputevents
where itemid = 221653 -- dobutamine