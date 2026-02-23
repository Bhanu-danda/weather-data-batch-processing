
CREATE TABLE weather_observations (
    observation_time TIMESTAMP PRIMARY KEY,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT,
    weathercode INT,
    ingestion_time TIMESTAMP,
    run_id UUID
);

select * from  weather_observations;

Delete from weather_observations;

select * from  weather_observations;




