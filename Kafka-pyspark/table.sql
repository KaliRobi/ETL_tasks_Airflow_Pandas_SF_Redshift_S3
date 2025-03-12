CREATE TABLE stg_viljandi_weather (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ,
    cloud_cover INT,
    cloud_cover_unit VARCHAR(10),
    wind_speed_10m DOUBLE PRECISION,
    wind_speed_10m_unit VARCHAR(10)
);