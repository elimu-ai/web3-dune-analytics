WITH
  details as (
    select
      0xe29797910d413281d2821d5d9a989262c8121cc2 as token,
      18 as dec
  ),
  multiplier AS (
    SELECT
      decimals as dec
    FROM
      tokens.erc20,
      details
    WHERE
      contract_address = token
  ),
  transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      sum(amount) AS amount
    FROM
      (
        SELECT
          date_trunc('day', evt_block_time) AS DAY,
          to AS address,
          tr.contract_address AS token_address,
          cast(value as int256) AS amount
        FROM
          erc20_ethereum.evt_Transfer tr,
          details
        WHERE
          contract_address = token
        UNION ALL
        SELECT
          date_trunc('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          - cast(value as INT256) AS amount
        FROM
          erc20_ethereum.evt_Transfer tr,
          details
        WHERE
          contract_address = token
      ) as t
    GROUP BY
      1,
      2,
      3
  ),
  balances_with_gap_days AS (
    SELECT
      t.day as day,
      address,
      SUM(amount) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day asc
      ) AS balance,
      lead(DAY, 1, now()) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day asc
      ) AS next_day
    FROM
      transfers t
  ),
  days AS (
    SELECT
      DAY
    FROM
      unnest (
        sequence(
          date('2021-06-01'),
          date(current_date),
          interval '1' day
        )
      ) as DAYS (DAY)
  ),
  tx AS (
    SELECT
      date_trunc('day', evt_block_time) AS DAY,
      COALESCE(sum(value / (power(10, dec))), 0) as supply
    FROM
      erc20_ethereum.evt_Transfer,
      details
    WHERE
      contract_address = token
      AND "from" = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
    UNION ALL
    SELECT
      date_trunc('day', evt_block_time) AS DAY,
      COALESCE(
        sum(- cast(value as INT256) / (power(10, dec))),
        0
      ) AS supply
    FROM
      erc20_ethereum.evt_Transfer,
      details
    WHERE
      contract_address = token
      AND to = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
  ),
  supply_table AS (
    SELECT
      DAY,
      sum(sum(t.supply)) over (
        order by
          DAY
      ) as Asset_Supply
    FROM
      (
        SELECT
          DAY,
          tx.supply as supply
        FROM
          tx
        UNION ALL
        SELECT
          DAY,
          0 as supply
        FROM
          days
      ) t
    GROUP BY
      1
    ORDER BY
      1 DESC
  ),
  balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance / (power(10, dec))) AS balance
    FROM
      details,
      balances_with_gap_days b INNER JOIN
      days d ON b.day <= d.day
      AND d.day < b.next_day
    WHERE
      balance / (power(10, dec)) > 0
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  )

SELECT
  b.day AS Date,
  COUNT(address) AS Holders,
  Asset_Supply
FROM
  balance_all_days b
  LEFT JOIN supply_table s ON b.day = s.DAY
GROUP BY
  1,
  3
ORDER BY
  1 DESC;
