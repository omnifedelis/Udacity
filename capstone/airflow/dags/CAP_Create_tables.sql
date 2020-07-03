DROP TABLE IF EXISTS public.accident;
CREATE TABLE IF NOT EXISTS public.accident (
    id              varchar,
    source          varchar         NOT NULL,
    tmc             varchar,
    severity        int             NOT NULL,
    starttime       timestamp       NOT NULL,
    date            varchar,
    time            varchar,
    distance        decimal(8,2)    NOT NULL,
    description     varchar(500)	NOT NULL,
    weather_id      varchar         NOT NULL,
    area_id         varchar         NOT NULL,
    CONSTRAINT accident_pkey PRIMARY KEY (id)
);
DROP TABLE IF EXISTS public.weather;
CREATE TABLE IF NOT EXISTS public.weather (
    weather_id 	    varchar,
    temp 			double precision,
    type            varchar,
    severity        varchar,
    wind_chill 		double precision,
    humidity 		double precision,
    pressure 		double precision,
    visibility 		double precision,
    wind_direction 	varchar,
    wind_speed 		double precision,
    precipitation 	double precision,
    airportcode     varchar,
    date            varchar,
    time            varchar,
    CONSTRAINT weather_pkey PRIMARY KEY (weather_id)
 );
DROP TABLE IF EXISTS public.location;
CREATE TABLE IF NOT EXISTS public.location (
    loc_id 		    varchar,
    street 			varchar,
    side 			varchar,
    city 			varchar,
    county 			varchar,
    state 			varchar(2),
    zipcode 		varchar,
    country 		varchar(2),
    lat				decimal(8,4),
    long			decimal(8,4),
    CONSTRAINT location_pkey PRIMARY KEY (loc_id)
);
DROP TABLE IF EXISTS public.time;
CREATE TABLE IF NOT EXISTS public.time (
    start_time 	timestamp,
    hour 		int,
    day 		int,
    week 		int,
    month 		int,
    year 		int,
    weekday 	int,
    CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
DROP TABLE IF EXISTS public.area_poi;
CREATE TABLE IF NOT EXISTS public.area_poi (
    poi_id  		varchar,
    amenity 		boolean,
    bump 			boolean,
    crossing 		boolean,
    give_way 		boolean,
    junction 		boolean,
    no_exit 		boolean,
    railway 		boolean,
    roundabout 		boolean,
    station 		boolean,
    stop 			boolean,
    trfc_calm 		boolean,
    trfc_sig 		boolean,
    turn_lp 		boolean,
    sunrise_set 	varchar,
    civ_twi 		varchar,
    naut_twi 		varchar,
    astro_twi 		varchar,
    CONSTRAINT area_poi_pkey PRIMARY KEY (poi_id)
);




