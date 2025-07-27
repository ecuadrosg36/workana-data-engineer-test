CREATE TABLE IF NOT EXISTS fact_transacciones (
    id SERIAL PRIMARY KEY,
    user_id INT,
    monto NUMERIC,
    fecha TIMESTAMP,
    estado VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_usuario (
    user_id INT PRIMARY KEY,
    nombre VARCHAR(100)
);
