create table if not exists state (
  state_id int,
  name text,
  geometry blob,
  median_income int,
  primary key (state_id)
);

create table if not exists county (
  county_id int,
  state_id int,
  name text,
  geometry blob,
  median_income int,
  primary key (county_id, state_id)
);
