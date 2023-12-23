WITH getBalance as ( 
select day as date,
       pool_id as pool_address,
       CASE WHEN token_address = 0xe29797910D413281d2821D5d9a989262c8121CC2 THEN 'ELIMU' ELSE token_symbol END as symbol,
       CASE WHEN token_address = 0xe29797910D413281d2821D5d9a989262c8121CC2 THEN pool_liquidity_eth END as balance
from balancer_v2_ethereum.liquidity
where pool_id IN (0x517390b2b806cb62f20ad340de6d98b2a8f17f2b0002000000000000000001ba)
and day >= CAST('2021-07-01' AS TIMESTAMP)
),

balances_with_gap_days AS (
SELECT t.date,
       pool_address,
       symbol,
       balance,
       LEAD(t.date, 1, NOW()) OVER (PARTITION BY pool_address, symbol ORDER BY t.date) AS next_day /* the day after a day with a transfer */
FROM getBalance AS t
),

days AS (
SELECT date FROM UNNEST(sequence(CAST('2021-07-01' AS TIMESTAMP), CAST(NOW() AS TIMESTAMP), interval '1' day)) as tbl(date)
)

SELECT d.date,
       pool_address,
       symbol,
       SUM(balance) AS balance
FROM balances_with_gap_days AS b
INNER JOIN days AS d ON b.date <= d.date
AND d.date < b.next_day /* Yields an observation for every day after the first transfer until the next day with transfer */
GROUP BY 1, 2, 3
ORDER BY 1
