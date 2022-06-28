WITH
  weth_price AS (
    SELECT
      date_trunc('DAY', minute) AS datex,
      AVG(price) AS weth_price
    FROM
      prices."usd"
    WHERE
      symbol = 'WETH' --WETH
    GROUP BY
      1
  ),
  ratio AS (
    SELECT
      datex,
      AVG(elimu_eth_ratio) AS elimu_eth_ratio
    FROM
      (
        SELECT
          date_trunc('DAY', evt_block_time) AS datex,
          ABS("amount0Out" / "amount1In") AS elimu_eth_ratio
        FROM
          uniswap_v2."Pair_evt_Swap"
        WHERE
          contract_address = '\xa0d230Dca71a813C68c278eF45a7DaC0E584EE61'
          AND "amount1In" != 0
        UNION
        SELECT
          date_trunc('DAY', evt_block_time) AS datex,
          ABS("amount0In" / "amount1Out") AS elimu_eth_ratio
        FROM
          uniswap_v2."Pair_evt_Swap"
        WHERE
          contract_address = '\xa0d230Dca71a813C68c278eF45a7DaC0E584EE61'
          AND "amount1Out" != 0
      ) x
    GROUP BY
      1
  ),
  elimu_price AS (
    SELECT
      r.datex,
      AVG(elimu_eth_ratio * weth_price) AS elimu_price
    FROM
      ratio r
      LEFT JOIN weth_price p ON r.datex = p.datex
    GROUP BY
      1
  ),
  -- tokens AS
  -- (SELECT
  --     datex,
  --     SUM(elimu) OVER (
  --         ORDER BY
  --           datex ASC
  --       ) AS elimu,
  --       SUM(weth) OVER (
  --         ORDER BY
  --           datex ASC
  --       ) AS weth
  -- FROM
  -- (SELECT
  --     datex,
  --     SUM(elimu) AS elimu,
  --     SUM(weth) AS weth
  -- FROM
  -- (SELECT
  --         date_trunc('DAY', COALESCE(t1.evt_block_time, t2.evt_block_time)) AS datex,
  --         (-1) * t1.value / 10 ^ (18) AS elimu,
  --         (-1) * t2.value / 10 ^ (18) AS weth
  --         FROM
  --           erc20."ERC20_evt_Transfer" t1
  --           FULL JOIN erc20."ERC20_evt_Transfer" t2 ON t1.evt_block_time = t2.evt_block_time
  -- WHERE
  --     t1."from" = '\xa0d230dca71a813c68c278ef45a7dac0e584ee61'
  --     AND t1.contract_address = '\xe29797910d413281d2821d5d9a989262c8121cc2'
  --     AND t2."from" = '\xa0d230dca71a813c68c278ef45a7dac0e584ee61'
  --     AND t2.contract_address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
  --     UNION
  -- SELECT
  --         date_trunc('DAY', COALESCE(t1.evt_block_time, t2.evt_block_time)) AS datex,
  --         t1.value / 10 ^ (18) AS elimu,
  --         t2.value / 10 ^ (18) AS weth
  --         FROM
  --           erc20."ERC20_evt_Transfer" t1
  --           FULL JOIN erc20."ERC20_evt_Transfer" t2 ON t1.evt_block_time = t2.evt_block_time
  -- WHERE
  --     t1."to" = '\xa0d230Dca71a813C68c278eF45a7DaC0E584EE61'
  --     AND t1.contract_address = '\xe29797910d413281d2821d5d9a989262c8121cc2'
  --     AND t2."to" = '\xa0d230Dca71a813C68c278eF45a7DaC0E584EE61'
  --     AND t2.contract_address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') x
  --     GROUP BY datex
  --     ) y
  -- )
  tokens AS (
    SELECT
      datex,
      LAST_VALUE(reserve0 / 10 ^ 18) OVER (
        PARTITION BY datex
        ORDER BY
          datex DESC
      ) AS weth,
      LAST_VALUE(reserve1 / 10 ^ 18) OVER (
        PARTITION BY datex
        ORDER BY
          datex DESC
      ) AS elimu
    FROM
      (
        SELECT
          date_trunc('DAY', evt_block_time) AS datex,
          reserve0,
          reserve1
        FROM
          uniswap_v2."Pair_evt_Sync"
        WHERE
          contract_address = '\xa0d230Dca71a813C68c278eF45a7DaC0E584EE61'
          AND evt_block_time > '2021-12-12'
      ) x
  )
SELECT
  t.datex,
  elimu_price,
  elimu,
  elimu * elimu_price AS elimu_liquidity,
  weth_price,
  weth,
  weth * weth_price AS weth_liquidity,
  elimu * elimu_price + weth * weth_price AS "Uniswap elimu.ai-WETH Pool $TVL"
FROM
  tokens t
  INNER JOIN elimu_price p1 ON p1.datex = t.datex
  LEFT JOIN weth_price p2 ON p2.datex = t.datex
ORDER BY
  1 DESC
