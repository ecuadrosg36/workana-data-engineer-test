DROP VIEW IF EXISTS anomalies_by_volume;

CREATE VIEW anomalies_by_volume AS
WITH daily AS (
  SELECT DATE(ts) AS tx_date, COUNT(*) AS total_tx
  FROM transactions
  GROUP BY 1
)
, calc AS (
  SELECT
    tx_date,
    total_tx,
    -- promedio de los últimos 3 días previos
    AVG(total_tx) OVER (
      ORDER BY tx_date
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS avg_prev_3,
    -- desviación vs promedio
    total_tx
      - AVG(total_tx) OVER (
          ORDER BY tx_date
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS diff_vs_avg_3
  FROM daily
)
SELECT
  tx_date,
  total_tx,
  avg_prev_3,
  ROUND( (total_tx - avg_prev_3) * 100.0 / avg_prev_3, 2 ) AS pct_diff
FROM calc
WHERE avg_prev_3 IS NOT NULL
  AND ABS(total_tx - avg_prev_3) > (avg_prev_3 * 0.02)   -- *** 2% de desviación ***
ORDER BY tx_date;
