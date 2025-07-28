DROP VIEW IF EXISTS frequent_failures_last7days;

CREATE VIEW frequent_failures_last7days AS
SELECT
  user_id,
  COUNT(*) AS num_failed
FROM transactions
WHERE status = 'FAILED'
  AND DATE(ts) >= DATE('2025-07-01', '-7 days')
GROUP BY user_id
HAVING COUNT(*) > 3;
