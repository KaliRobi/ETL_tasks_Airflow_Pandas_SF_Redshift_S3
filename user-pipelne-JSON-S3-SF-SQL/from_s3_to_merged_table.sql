CREATE DATABASE mege_project;

use database mege_project;

show schemas;

create  schema mege_schema;

use schema mege_schema;

-- create storarge integraion 
create storage integration mege_integration
type = EXTERNAL_STAGE
storage_provider = S3
enabled = True
storage_aws_role_arn = 'arn:aws:iam::842676000276:role/allow-access-from-SF'
storage_allowed_locations = ('S3://snowflake-external-bucket/');



select SYSTEM$VALIDATE_STORAGE_INTEGRATION( 'MEGE_INTEGRATION', 'S3://snowflake-external-bucket/', 'data2.json', 'read' );

grant create stage on schema mege_schema to role DATAENGINEER;

grant usage on integration MEGE_INTEGRATION to role DATAENGINEER;

--create stage upon the integraion

Create or replace  stage mege_stage
storage_integration = mege_integration
url = 'S3://snowflake-external-bucket'
file_format = (type = JSON  );



-- create table load with and without transformation

create or replace temporary table data_source_one(
id NUMBER,
full_name VARCHAR,
email VARCHAR,
status VARCHAR,
created_at  TIMESTAMP
) as
select 
value:id::INT, 
value:name::STRING,
value:email::STRING, 
NULL as status,
TO_TIMESTAMP(value:created_at::STRING) as created_at
from @mege_stage/data_source_one.json sf
,table(flatten(input => parse_json($1)));


create or replace temporary table data_source_two(
ID NUMBER,
FULL_NAME VARCHAR,
STATUS VARCHAR,
CREATED_AT  TIMESTAMP
);

CREATE OR REPLACE FILE FORMAT json_format 
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;


-- sequence for the merged id
create sequence local_seq
start = 0
increment = 1;


copy into data_source_two
from @mege_stage
files = ('data_source_two.json')
MATCH_BY_COLUMN_NAME = case_insensitive
FILE_FORMAT = json_format;


-- merge and update the row sof the table

create or replace procedure merge_and_update()
returns string
language sql
as
$$
begin
    -- perform the merge
merge into data_source_one as ds1
using data_source_two as ds2
on ds1.full_name = ds2.full_name
when matched then
update set
    ds1.id = local_seq.nextval,
    ds1.status = ds2.status
when not matched then
    insert(id, full_name, email, status, created_at )
    values(local_seq.nextval, ds2.full_name, 'unknown', ds2.status, ds2.created_at);

    --  update data_source_one where status is missing
    update data_source_one as ds2
    set status = 'tbd'
    where status is null;

    return 'merge and update completed successfully.';
end;
$$;

call merge_and_update();

--  section to validate the steps

describe integration mege_integration;

show integrations;

select 
*
from @mege_stage/data_source_one.json sf
,table(flatten(input => parse_json($1)));

select  * from data_source_one

order by full_name;

show file formats;

select * from data_source_two;

show stages;

list @mege_stage;


create table data_sample as
select * from @mege_stage/data2.json;

select * from data_sample;