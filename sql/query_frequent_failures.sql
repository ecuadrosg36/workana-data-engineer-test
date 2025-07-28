SELECT
  user_id,
  COUNT(*) AS num_failed
FROM transactions
WHERE status = 'FAILED'
  AND DATE(ts) >= DATE('now', '-7 days')
GROUP BY user_id
HAVING COUNT(*) > 3;
