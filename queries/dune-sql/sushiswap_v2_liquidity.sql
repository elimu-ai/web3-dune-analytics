WITH price AS(
WITH data AS (
    SELECT
    DATE_TRUNC('day', block_time) AS time,
      CASE
        WHEN token_bought_address = 0xe29797910D413281d2821D5d9a989262c8121CC2 THEN amount_usd / CAST((token_bought_amount_raw / 1e18) AS DOUBLE)
        ELSE amount_usd / CAST((token_sold_amount_raw / 1e18) AS DOUBLE)
      END AS elimu_price_usd
    FROM dex.trades
    WHERE
      blockchain = 'ethereum'
      AND token_bought_address = 0xe29797910D413281d2821D5d9a989262c8121CC2
      OR token_sold_address = 0xe29797910D413281d2821D5d9a989262c8121CC2
      AND ( token_sold_amount_raw <> CAST(0 as UINT256) AND token_bought_amount_raw <> CAST(0 as UINT256)
      )
      AND CASE
        WHEN token_bought_address = 0xe29797910D413281d2821D5d9a989262c8121CC2 THEN token_bought_amount_raw > CAST(100 as UINT256)
        ELSE token_sold_amount_raw > CAST(100 as UINT256)
      END /* filter out strange transactions */
    ORDER BY
      time NULLS FIRST
  )
SELECT
  time,
  AVG(elimu_price_usd) AS Price_USD
FROM data
WHERE time > CAST('2021-07-01' AS TIMESTAMP)
GROUP BY 1
ORDER BY 1 NULLS FIRST
),

elimu_pool_bal AS(
SELECT SUM(amount) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS ELIMU, day
FROM(
    SELECT SUM(amount/1e18) AS amount, day
    FROM(
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xe29797910D413281d2821D5d9a989262c8121CC2
    AND "to" = 0x0E2a3d127EDf3BF328616E02F1DE47F981Cf496A
            
    UNION ALL 
            
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        -1 * CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xe29797910D413281d2821D5d9a989262c8121CC2
    AND "from" = 0x0E2a3d127EDf3BF328616E02F1DE47F981Cf496A
    )
    GROUP BY day
    )
),

weth_pool_bal AS(
SELECT SUM(amount) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS weth, day
FROM(
    SELECT SUM(amount/1e18) AS amount, day
    FROM(
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    AND "to" = 0x0E2a3d127EDf3BF328616E02F1DE47F981Cf496A
            
    UNION ALL 
            
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        -1 * CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    AND "from" = 0x0E2a3d127EDf3BF328616E02F1DE47F981Cf496A
    )
    GROUP BY day
)
)

SELECT elimu, b.day, (elimu * price_usd) + weth AS liquidity, weth
FROM elimu_pool_bal b
LEFT JOIN weth_pool_bal ub ON b.day = ub.day
LEFT JOIN price p ON b.day = p.time;
