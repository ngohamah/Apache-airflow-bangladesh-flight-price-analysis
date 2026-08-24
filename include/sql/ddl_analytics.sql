-- PostgreSQL analytics schema. Idempotent: safe to run on every DAG run.
-- All tables carry batch_id so a rerun can delete-then-insert its own rows only.

CREATE TABLE IF NOT EXISTS flights_clean (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id TEXT NOT NULL,
    airline TEXT,
    source TEXT,
    source_name TEXT,
    destination TEXT,
    destination_name TEXT,
    departure_dt TIMESTAMP,
    arrival_dt TIMESTAMP,
    duration_hrs DOUBLE PRECISION,
    stopovers TEXT,
    aircraft_type TEXT,
    class TEXT,
    booking_source TEXT,
    base_fare DOUBLE PRECISION,
    tax_surcharge DOUBLE PRECISION,
    total_fare DOUBLE PRECISION,
    seasonality TEXT,
    days_before_departure INTEGER
);
CREATE INDEX IF NOT EXISTS idx_flights_clean_batch_id ON flights_clean (batch_id);

CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    batch_id TEXT NOT NULL,
    airline TEXT NOT NULL,
    avg_total_fare NUMERIC
);
CREATE INDEX IF NOT EXISTS idx_kpi_avg_fare_by_airline_batch_id ON kpi_avg_fare_by_airline (batch_id);

CREATE TABLE IF NOT EXISTS kpi_seasonal_fare_variation (
    batch_id TEXT NOT NULL,
    seasonality TEXT NOT NULL,
    avg_total_fare NUMERIC,
    booking_count BIGINT
);
CREATE INDEX IF NOT EXISTS idx_kpi_seasonal_fare_variation_batch_id ON kpi_seasonal_fare_variation (batch_id);

CREATE TABLE IF NOT EXISTS kpi_bookings_by_airline (
    batch_id TEXT NOT NULL,
    airline TEXT NOT NULL,
    booking_count BIGINT
);
CREATE INDEX IF NOT EXISTS idx_kpi_bookings_by_airline_batch_id ON kpi_bookings_by_airline (batch_id);

CREATE TABLE IF NOT EXISTS kpi_top_routes (
    batch_id TEXT NOT NULL,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    booking_count BIGINT,
    rank INTEGER
);
CREATE INDEX IF NOT EXISTS idx_kpi_top_routes_batch_id ON kpi_top_routes (batch_id);
