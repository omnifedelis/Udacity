DROP TABLE IF EXISTS public.accident;
CREATE TABLE IF NOT EXISTS public.accident (
    accident_id     varchar,
    type            varchar         NOT NULL,
    severity        varchar,
    tmc             varchar         NOT NULL,
    description     varchar(2000)   NOT NULL,
    starttime       timestamp       NOT NULL,
    date            varchar,
    time            varchar,
    distance        decimal(8,2),
    CONSTRAINT accident_pkey PRIMARY KEY (accident_id)
);
DROP TABLE IF EXISTS public.weather;
CREATE TABLE IF NOT EXISTS public.weather (
    weather_id 	    varchar,
    type            varchar,
    severity        varchar,
    weather_start   timestamp,
    date            varchar,
    time            varchar,
    airportcode     varchar,
    city            varchar,
    state           varchar(2),
    zipcode         varchar,
    CONSTRAINT weather_pkey PRIMARY KEY (weather_id)
 );
DROP TABLE IF EXISTS public.location;
CREATE TABLE IF NOT EXISTS public.location (
    loc_id          varchar,
    number          varchar,
    street 	    varchar,
    side            varchar,
    city 	    varchar,
    county 	    varchar,
    state 	    varchar(2),
    zipcode 	    varchar,
    lat		    decimal(8,4),
    long	    decimal(8,4),
    CONSTRAINT location_pkey PRIMARY KEY (loc_id)
);
DROP TABLE IF EXISTS public.time;
CREATE TABLE IF NOT EXISTS public.time (
    starttime 	    timestamp,
    hour 	    int,
    day 	    int,
    week 	    int,
    month 	    int,
    year 	    int,
    weekday 	    int,
    CONSTRAINT time_pkey PRIMARY KEY (starttime)
);





