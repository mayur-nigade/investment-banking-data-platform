-- dbt singular test: each trade's reconstructed version chain
-- must have exactly one current (highest sequence) row.
-- Zero returned records => PASS.
SELECT TRADE_ID, COUNT_IF(IS_CURRENT) AS CURRENT_VERSIONS
FROM {{ ref('fct_trade_state_history') }}
GROUP BY TRADE_ID
HAVING COUNT_IF(IS_CURRENT) <> 1
