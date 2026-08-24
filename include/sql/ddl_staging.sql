-- MySQL staging schema. Idempotent: safe to run on every DAG run.

CREATE TABLE IF NOT EXISTS flights_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(150),
    destination VARCHAR(10),
    destination_name VARCHAR(150),
    departure_dt DATETIME,
    arrival_dt DATETIME,
    duration_hrs DOUBLE,
    stopovers VARCHAR(20),
    aircraft_type VARCHAR(50),
    class VARCHAR(20),
    booking_source VARCHAR(50),
    base_fare DOUBLE,
    tax_surcharge DOUBLE,
    total_fare DOUBLE,
    seasonality VARCHAR(30),
    days_before_departure INT,
    INDEX idx_flights_raw_batch_id (batch_id)
);

CREATE TABLE IF NOT EXISTS flights_rejected (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    raw_row JSON,
    rejection_reason VARCHAR(255),
    INDEX idx_flights_rejected_batch_id (batch_id)
);
