{{ config(
    materialized='table',
    full_refresh=True
) }}


select
    *
from {{ ref('user_details') }}
where
    pt=date_add(date(current_timestamp()),interval -1 day)
    and same_domain_users>=2
    and domain not in (
        'yahoo.co.jp','yahoo.com','googlemail.com','163.com','outlook.jp','qq.com','i.softbank.jp','ezweb.ne.jp','docomo.ne.jp','me.com','hotmail.co.jp','aol.com','hotmail.fr','live.com','nifty.com','yahoo.fr','hotmail.co.uk','mac.com','msn.com','softbank.ne.jp'
        ,'yopmail.com','outlook.fr','yahoo.ne.jp','g.softbank.co.jp','comcast.net')
    and signup_country in ('Japan','United States')
    and total_record_count>0
    and is_paid=1