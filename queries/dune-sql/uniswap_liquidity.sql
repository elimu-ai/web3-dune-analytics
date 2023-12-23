WITH price AS(
WITH data AS (
    SELECT
    DATE_TRUNC('day', block_time) AS time,
      CASE
        WHEN token_bought_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD THEN amount_usd / CAST((token_bought_amount_raw / 1e18) AS DOUBLE)
        ELSE amount_usd / CAST((token_sold_amount_raw / 1e18) AS DOUBLE)
      END AS stbu_price_usd
    FROM dex.trades
    WHERE
      blockchain = 'ethereum'
      AND token_bought_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD
      OR token_sold_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD
      AND ( token_sold_amount_raw <> CAST(0 as UINT256) AND token_bought_amount_raw <> CAST(0 as UINT256)
      )
      AND CASE
        WHEN token_bought_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD THEN token_bought_amount_raw > CAST(100 as UINT256)
        ELSE token_sold_amount_raw > CAST(100 as UINT256)
      END /* filter out strange transactions */
    ORDER BY
      time NULLS FIRST
  )
SELECT
  time,
  AVG(stbu_price_usd) AS Price_USD
FROM data
WHERE time > CAST('2022-02-01' AS TIMESTAMP)
GROUP BY 1
ORDER BY 1 NULLS FIRST
),

stbu_pool_bal AS(
SELECT SUM(amount) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS STBU, day
FROM(
    SELECT SUM(amount/1e18) AS amount, day
    FROM(
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD
    AND "to" = 0x390A4D096BA2CC450E73B3113F562be949127ceB
            
    UNION ALL 
            
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        -1 * CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xa6422E3E219ee6d4C1B18895275FE43556fd50eD
    AND "from" = 0x390A4D096BA2CC450E73B3113F562be949127ceB
    )
    GROUP BY day
    )
),

usdt_pool_bal AS(
SELECT SUM(amount) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS usdt, day
FROM(
    SELECT SUM(amount/1e6) AS amount, day
    FROM(
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7
    AND "to" = 0x390A4D096BA2CC450E73B3113F562be949127ceB
            
    UNION ALL 
            
    SELECT 
        date_trunc('day', evt_block_time) AS day,
        -1 * CAST(value AS int256) AS amount
    FROM erc20_ethereum.evt_Transfer tr 
    WHERE contract_address = 0xdAC17F958D2ee523a2206206994597C13D831ec7
    AND "from" = 0x390A4D096BA2CC450E73B3113F562be949127ceB
    )
    GROUP BY day
)
)

SELECT stbu, b.day, (stbu * price_usd) + usdt AS liquidity, usdt
FROM stbu_pool_bal b
LEFT JOIN usdt_pool_bal ub ON b.day = ub.day
LEFT JOIN price p ON b.day = p.time;


            
