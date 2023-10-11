WITH 
multiplier AS 
        (
        SELECT decimals as dec FROM tokens.erc20
        WHERE contract_address = lower('{{Token_Address}}')
        ),
transfers AS
(
SELECT  DAY, 
        address,
        token_address,
        sum(amount) AS amount 
FROM
    (
    SELECT  date_trunc('day', evt_block_time) AS DAY,
            to AS address,
            tr.contract_address AS token_address,
            value AS amount 
    FROM erc20_ethereum.evt_Transfer tr
    WHERE contract_address = lower('{{Token_Address}}')
    
    UNION ALL
    
    SELECT  date_trunc('day', evt_block_time) AS DAY,
            from AS address,
            tr.contract_address AS token_address,
            -value AS amount
    FROM erc20_ethereum.evt_Transfer tr
    WHERE contract_address = lower('{{Token_Address}}')
    ) as t
GROUP BY 1, 2, 3
),
balances_with_gap_days AS
(
SELECT  t.day,
        address,
        SUM(amount) OVER (PARTITION BY address ORDER BY t.day asc) AS balance, 
        lead(DAY, 1, now() ) OVER (PARTITION BY address ORDER BY t.day asc) AS next_day 
FROM transfers t
),
days AS
(
SELECT  explode(sequence(to_date('2016-01-20'), to_date(current_date), interval 1 day)) AS DAY
),
tx AS
    (
    SELECT
    date_trunc('day', evt_block_time) AS DAY,
    COALESCE(sum(value/(power(10,dec))), 0) as supply
    FROM erc20_ethereum.evt_Transfer, multiplier
    WHERE contract_address = lower('{{Token_Address}}')
    AND from = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) AS DAY,
    COALESCE(sum(-value/(power(10,dec))), 0) AS supply
    FROM erc20_ethereum.evt_Transfer, multiplier
    WHERE contract_address = lower('{{Token_Address}}')
    AND to = '0x0000000000000000000000000000000000000000'
    GROUP BY 1
    ),
supply_table AS    
    (
    SELECT DAY,
    sum(sum(t.supply)) over (order by DAY) as Asset_Supply
    FROM 
        (
        SELECT DAY, tx.supply as supply 
        FROM tx 
    UNION ALL
        SELECT DAY, 0 as supply 
        FROM days
        ) t
    GROUP BY 1
    ORDER BY 1 DESC
    ),
balance_all_days AS
(
SELECT  d.day,
        address,
        SUM(balance/(power(10,dec))) AS balance
FROM balances_with_gap_days b, multiplier
   INNER JOIN days d ON b.day <= d.day
AND d.day < b.next_day 
WHERE balance/(power(10,dec)) > 0
GROUP BY 1,2
ORDER BY 1,2 
)
SELECT  b.day AS Date,
        COUNT(address) AS Holders,
        Asset_Supply
FROM balance_all_days b
LEFT JOIN supply_table s ON b.day = s.DAY
GROUP BY 1,3
ORDER BY 1 DESC ;
