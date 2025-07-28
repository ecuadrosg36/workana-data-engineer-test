-- Trigger: detectar fechas futuras
CREATE TRIGGER IF NOT EXISTS trg_check_future_date
BEFORE INSERT ON transactions
FOR EACH ROW
BEGIN
  SELECT 
    CASE 
      WHEN NEW.ts > datetime('now') THEN
        RAISE(ABORT, '❌ La fecha de la transacción está en el futuro.')
    END;
END;

-- Trigger: detectar duplicados por order_id
CREATE TRIGGER IF NOT EXISTS trg_prevent_duplicate_order
BEFORE INSERT ON transactions
FOR EACH ROW
BEGIN
  SELECT 
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM transactions WHERE order_id = NEW.order_id
      ) THEN
        RAISE(ABORT, '❌ order_id duplicado.')
    END;
END;
