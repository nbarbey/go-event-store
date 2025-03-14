create schema if not exists string_events authorization "postgres";
grant all privileges on schema string_events to "postgres";

create schema if not exists my_events authorization "postgres";
grant all privileges on schema my_events to "postgres";

create schema if not exists todo_events authorization "postgres";
grant all privileges on schema todo_events to "postgres";

create schema if not exists items_events authorization "postgres";
grant all privileges on schema items_events to "postgres";
