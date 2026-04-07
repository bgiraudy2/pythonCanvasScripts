This is a project that I worked on while at Florida State University. This project downloaded data from Canvas and databases and loaded them into tables and Canvas. 

# pythonReplicateData

## downloadtables.py
This CLI application works by utilizing cx_Oracle to run a query in order to get results for a specific table (from a YAML list or a specificied table in the command line). 

The query results are then placed in a Pandas table, and then downloaded as a .dat file.

In order to run the application independently, cd into the pythonReplicateData project and the command should look like:

```bash
# for all of the tables in a list based on the schedule type (in addition to which db to grab the data from)
python3.6 downloadtables.py schedule db
username@hostname > python3.6 downloadtables.py {all, hourly, daily, weekly, qna} {odl_usr, dbprod, dbtest, qna}

# ex: 'python3.6 downloadtables.py all qna' will download all of the "all" tables from the tablelist.yaml file using the qna db.

# to download one table from a list based on the schedule type (in addition to which db to grab the data from):
username@hostname > python3.6 downloadtables.py --tablename {tablename} {all, hourly, daily, weekly, qna} {odl_usr, dbprod, dbtest, qna}
# ex: 'python3.6 downloadtables.py --tablename tmp_example_table custom qna' will only download the data for the tmp_example_table table from the "custom" tables list using the qna db.
```
The '--tablename' argument is an optional but important one if you are looking to download the data of the tmp_table of your choice. 
## sqlldrData.py


```bash
# for all of the tables in a list based on the schedule type
python3.6 downloadtables.py schedule
username@hostname > python3.6 sqlldrData.py {all, hourly, daily, weekly, qna}

# ex: 'python3.6 sqlldrData.py all qna' will load and refresh all of the "custom" tables from the tablelist.yaml file

# to load and refresh one table from a list based on the schedule type:
username@hostname > python3.6 sqlldrData.py --tablename {tablename} {all, hourly, daily, weekly, qna}
# ex: 'python3.6 sqlldrData.py --tablename tmp_campus all' will only load and refresh the data for the fsuvs_example_table mat view from the "custom" tables list.
```

## Adding New Tables
### conf/appconf.yaml 
There is a possibility that you might have to add a new user, but for the most part that is a rare occurrance. What isn't unusual though, is having to change the password of an already existing user. Especially after an Oracle upgrade.

In order to change the password of a user, you will have to encode it first and paste the encoded password into the appconf yaml file.

In order to do so, you must run the 'encoder_decoder.py' script:

For example:

```bash
$ python3 encoder_decoder.py {encode|decode} {password}

#to encode a password:
$ python3 encoder_decoder.py encode hello
aGVsbG8=

# to decode a password:
$ python3 encoder_decoder.py decode aGVsbG8=
hello
```

The password in the YAML file must be encoded, as the password is decoded in the script directly.

### conf/tablelist.yaml

After verifying that the appconf.yaml looks good, you have to add the tables that you are interesting in grabbing data from to the tablelist.yaml. Depending on how often you want this table to run, it's ideal to also include it to the 'all' schedule, since you are able to download/refresh one or more tables. Make sure these tables are in alphabetical order:

```yaml
---
# tables to be loaded via script argument 

all:
  - tmp_example_table
  - tmp_example_table2
  - tmp_example_table3
...
```


### tmp_table / materialized view creation

The next step would be to create the base temp tables for the data to be stored in, along with the materialized view that will be used to query the data. 

The temp tables will need to be created in schema A, and then the materialized view will need to be created in schema B, selecting the data from schema A:

-- base tables 

```sql
-- example
CREATE TABLE "TMP_EXAMPLE_TABLE" 
("ID"	VARCHAR2(5),
"COURSE_ID"	NUMBER(38),
"SEMESTER"	VARCHAR2(4),
"COURSE_NAME"	VARCHAR2(20)
);

```
-- materialized views 

```sql
CREATE MATERIALIZED VIEW SCHEMA_B.FSUVS_EXAMPLE_TABLE AS
SELECT "ID", "COURSE_ID", "SEMESTER", "COURSE_NAME" FROM SCHEMA_A.TMP_EXAMPLE_TABLE;

```

### conf/tablesconf.yaml

Once the base tables and the materialized views are created, we will need to place the select queries inside of the 'tablesconf.yaml' file. Inside of tablesconf, the format should look similar to this:

```yaml
# example
terms:
  earlyenroll: "'2201'"
  current: "'2199'"
  previous: "'2196'"
  next: "'2201'"
  upcoming: "('2199', '2201')"
  terms: "('2196', '2199', '2201')"
  blueprint: "'2196'"

tmp_example_table:
  cstable: "tmp_example_table"
  mvtable: "fsuvs_example_table"
  dctable: "full_example_table"
  varbl1: ""
  sql: "SELECT "ID", "COURSE_ID", "SEMESTER", "COURSE_NAME" FROM original_schema.full_example_table "
  toggle: "on"
```
If the table requires a varbl1, place it inside of the varbl1 value. The script will look at the terms keys and convert the '$varbl1' placeholder inside of the sql query to match whichever the varbl1 key is for the table. In this case, '$varbl1' is set to 'upcoming', which is "('2199', '2201')". In other words, this is set to the (prev, next) semester.


cstable = the base table that the data will be loaded inside of.
mvtable = the materialized view in which the data will be able to be queried from
dctable = the source of the data, if you will.
varbl1 = the terms that you wish to grab the data from. This is optional and depends on which tables require term data.
sql = the query that will be used to load the data to the base table. this will be queried from the original source of the data 'dctable'.
toggle = if the toggle is on, it will load the data into the base table, if it's off it will skip the table and change the status table to say that the table was 'skipped'.

### ctls/tmp_{tablename}.ctl

In order to SQLLOAD the table, you will need to create a .ctl file for it, which specifies where the data is coming from, where the bad data is going, and which table the data will be loaded into. 

```sql
LOAD DATA
CHARACTERSET UTF8
INFILE '{fullfilepath}/dat/tmp_example_table.dat' "STR '#EOR#\n'"
BADFILE '{fullfilepath}/bad/tmp_example_table.bad'
TRUNCATE
INTO TABLE tmp_example_table
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
ID,
COURSE_ID,
SEMESTER,
COURSE_NAME
)
```

## replicate_current_status table

In order to see what the current status is of any table, you will have to include it in the replicate_current_status table. In SQL Developer, You will need to manually insert all of the tmp tables (CTRL-i), type each materialized view name 'fsuvs_{tablename}' under the 'jobname' column, and commit the changes (F11). In any other DBMS ide, include the names of the base tables in a column named 'tablename', and inside the name of the materialized view under the 'jobname' column, and commit the changes to the 'replicate_current_status' table.

## Run downloadtables.py

Go ahead and run the downloadtables.py script in order to download the .dat files:

```bash
username@hostname> python3.6 downloadtables.py all
Downloading tmp_example_table
tmp_example_table.dat downloaded.

Downloading tmp_example_table2
tmp_example_table2.dat downloaded.

Downloading tmp_example_table3
tmp_example_table3.dat downloaded.

```

## Run sqlldrData.py 

Lastly, run the sqlldrData.py script in order to load the mat views and refresh them, which will also update the replicate_status/replicate_current_status tables

```bash
username@hostname> python3.6 sqlldrData.py all
SQLLOADing the data.


Loading: tmp_example_table

SQL*Loader: Release 12.2.0.1.0 - Production on Mon Feb 10 16:30:32 2020

Copyright (c) 1982, 2017, Oracle and/or its affiliates.  All rights reserved.


  26421 Rows successfully loaded.
tmp_example_table SQLLOADED successfully!
Loading: tmp_example_table2

SQL*Loader: Release 12.2.0.1.0 - Production on Mon Feb 10 16:30:34 2020

Copyright (c) 1982, 2017, Oracle and/or its affiliates.  All rights reserved.


  34674 Rows successfully loaded.
tmp_example_table2 SQLLOADED successfully!
Loading: tmp_example_table3

SQL*Loader: Release 12.2.0.1.0 - Production on Mon Feb 10 16:30:36 2020

Copyright (c) 1982, 2017, Oracle and/or its affiliates.  All rights reserved.


  178018 Rows successfully loaded.
tmp_example_table3 SQLLOADED successfully!
Refreshing the materialized views in schema_b:


EXECUTE DBMS_MVIEW.REFRESH('schema_b.fsuvs_example_table', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)
MV fsuvs_example_table refreshed successfully!
UPDATE REPLICATE_CURRENT_STATUS SET STATUS = 'successful' WHERE JOBNAME = 'fsuvs_example_table'
EXECUTE DBMS_MVIEW.REFRESH('schema_b.example_table2', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)
MV fsuvs_example_table2 refreshed successfully!
UPDATE REPLICATE_CURRENT_STATUS SET STATUS = 'successful' WHERE JOBNAME = 'fsuvs_example_table2'
EXECUTE DBMS_MVIEW.REFRESH('schema_b.fsuvs_example_table3', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)
MV fsuvs_example_table3 refreshed successfully!
UPDATE REPLICATE_CURRENT_STATUS SET STATUS = 'successful' WHERE JOBNAME = 'fsuvs_example_table3'


```

## Verify

Verify that the data is loaded in both the tmp tables, as well as the materialized views. Should everything go well, you will have data in both the base table and mat view. Otherwise, check the .bad files/logs for any issues that need to be fixed.

# canvasProvisions.py

Provisioning reports download .csvs from Canvas, and this script will kick-start a report using the requests library in order to download the .csvs and place the data inside of cnv_* tables. Once the data is loaded, it will refresh the canvas_* materialized view. In order to run this script:

```bash
# To run a provision for all of the regular Canvas tables (which includes non-term and term-based data): 
python3.6 canvasProvisions.py  

# If you want to run a historical report, which are all of the terms that have ever existed in Canvas:
python3.6 canvasProvisions.py --historical true

# To run the org-based provisions, run:
python3.6 canvasProvisions.py --processorgs true
```

## conf/tablesconf.yaml

The only values that need to change would be for the terms key. Currently they are

```yaml
terms: ['sp20', 'fa19', 'su20']
```
These terms translate to be 'current', 'previous' and 'next'. Including other terms to this will download all of the .csvs from that semester. 

The terms require the 'human-readable' version of a Canvas term. The script will convert these values into their numerical version as long as the term key starts with 'xx00' (two letters and two numbers). As long as the term exists in Canvas, it will download all of the numerical terms for that term.

# canvasApi.py

The canvasAPI CLI is responsible for taking what was downloaded from the canvasProvisions CLI and loading them directly into Canvas using Canvas' REST API.

In order to run the canvasAPI CLI, do the following:

```python
# In order to run the regular canvas API:
username@hostname> python3.6 canvasApi.py

# In order to process the org enroll/unenrollments, do:
username@hostname> python3.6 canvasApi.py --processorgs true
```

The canvasApi CLI also has the ability to run singular processes as well. In order to do so, add the following argument.

```python
# In this instance, a tablename refers to the process that you wish to run as a singular job.
username@hostname> python3.6 canvasApi.py --tablename {process}

#example
username@hostname> python3.6 canvasApi.py --tablename canvas_example_table
username@hostname> python3.6 canvasApi.py --tablename canvas_example_table2
username@hostname> python3.6 canvasApi.py --tablename canvas_example_table3
```

# Crontabs

The crontabs are the method of running each application depending on how often you would need to do so. For instance, the "hourly" process is started every two hours, which will give each job (pythonReplicateData, pythonProvisions, pythonCanvasAPI) more than enough time to run. Each process usually takes about an hour and a half from beginning to end. The hourly was ran from 4am, 6am, 8am, 12pm, 2pm, 4pm, 6pm and 8pm every monday through friday.

That crontab entry would look like:

```bash
0 4,6,8,12,14,16,18,20 * * 1-5 /home/username/pythonCanvasScripts/crontabs/replicateHourly.sh
```

Where that replicateHourly.sh looks like:


```bash
NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas ReplicateHourly Script:" 
echo $NOW
echo "Running downloadtables.py hourly." 
cd {fullfilepath}/pythonReplicateData/
pwd
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/downloadtables.py hourly odl_usr 
sleep 2
echo "Now running SQLLDR along with a MV Refresh:" 
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/sqlldrData.py hourly
echo "Now running Provisioning script:"
python3.6 {fullfilepath}/pythonProvisions/canvasProvisions.py
provstatus=$(head -n 1 {fullfilepath}/pythonProvisions/provisions_status)
echo $provstatus
if [[ $provstatus == "successful" ]]
then
	echo "Now running CanvasAPI:"
	python3.6 {fullfilepath}/pythonCanvasAPI/canvasApi.py 
	echo "Hourly refresh completed."
	DONE=$( date '+%F_%H:%M:%S' )
	echo $DONE
elif [[ $provstatus == "failed" ]]
then
	echo "Provisions failed for some reason. Exiting crontab now!"
	python3.6 {fullfilepath}/pythonProvisions/provisions_fail_email.py
	exit 1
fi
```