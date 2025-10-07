create table state (
	state_id int,
	name text
);

create table county (
	county_id int,
	state_id int,
	name text
);

create table median_income if not exists (
	state_id int,
	county_id int,
	income int
);
