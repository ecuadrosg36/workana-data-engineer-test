DROP VIEW IF EXISTS summary_by_date_status;

CREATE VIEW summary_by_date_status AS
SELECT
  DATE(ts) AS fecha,
  status,
  COUNT(*) AS cantidad
FROM transactions
GROUP BY DATE(ts), status;
