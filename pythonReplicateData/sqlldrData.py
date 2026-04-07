#!/usr/bin/python3.6
import argparse
import cx_Oracle
import odlutils
from odlyaml import OdlYaml
import os
from pw_encoder_decoder import encoded, decoded
from replicate_logger import StatusLogger
import subprocess


## -------- ARGPARSE -------- ##
parser = argparse.ArgumentParser()

## jobtype is either of these arguments: {all, hourly, daily, weekly, qna}

parser.add_argument("jobtype")
parser.add_argument('--tablename', nargs='?', help="Insert table name that needs to be downloaded")
args = parser.parse_args()

jobtype = args.jobtype
tablename = args.tablename

## -------- DIRECTORY VARIABLES -------- ##
"""For security reasons, the full file path has been removed"""
basedir = "{fullfilepath}/pythonReplicateData"
confdir = basedir + "/conf/"
ctlsdir = basedir + "/ctls"
datadir = basedir + "/dat"
baddir = basedir + "/bad"
logdir = basedir + "/logs"

appconf = confdir + "appconf.yaml"
tablelistconf = confdir + "tablelist.yaml"
tablesconf = confdir + "tablesconf.yaml"

## -------- YAML VALUES -------- ##
tablelist = OdlYaml(tablelistconf)
tablelist_values = tablelist.values

tablelist = tablelist_values[jobtype]
max_val = len(tablelist)

jobs = tablelist[0:max_val]

appconf_yaml = OdlYaml(appconf)
appconf_values = appconf_yaml.values

tablesconf_yaml = OdlYaml(tablesconf)
tablesconf_values = tablesconf_yaml.values

## -------- SQLLDR INFO -------- ##
userid = appconf_values['userid']
passw = decoded(appconf_values['passw'])

## -------- ORACLE DB INFO -------- ##
o_userid = appconf_values['dbprod_user']
o_passw = decoded(appconf_values['dbprod_pw'])
o_url = appconf_values['dbprod_url']


oracle_connection = cx_Oracle.connect(o_userid, o_passw, o_url)
oracle_cursor = oracle_connection.cursor()

filename = os.path.basename(__file__)

# Load the data files into Oracle
print("SQLLOADing the data.\n\n")

if tablename is None:
	for jobname in jobs:
		toggle = tablesconf_values[jobname]['toggle']
		ctls = str(jobname) + ".ctl"
		data = str(jobname) + ".dat"
		bad = str(jobname) + ".bad"
		log = str(jobname) + ".log"

		print("Loading: " + jobname)

		datafile = datadir + '/' + data

		if jobname in jobs and toggle == 'on':
			"""for security reasons, the full path to sqlldr has been removed"""
			sqlldr = '{fullpathtosqlldr}/sqlldr CONTROL="' + ctlsdir + '/' + ctls  + '" LOG="' + logdir + '/' + log + '" BAD="' + baddir + '/' + bad + '", DATA="' + datadir + '/' + data + '" USERID=' + userid + '/' + passw + ' ERRORS=50 SKIP=0 SILENT=FEEDBACK DIRECT=TRUE'

			subprocess.call(sqlldr, shell=True)

			message = str("{} SQLLOADED successfully!".format(jobname))
			print(message)
			status = "successful"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)

			log_cursor.execute(log_query)
			log_conn.commit()
			log_conn.close()
		else:
			message = str("Cannot SQLLOAD {}: toggle switch is off. Loading next table.\n".format(jobname))
			print(message)
			status = "skipped"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)

			log_cursor.execute(log_query)
			log_conn.commit()
			log_conn.close()

elif tablename is not None:
	jobname = tablename

	try:
		toggle = tablesconf_values[jobname]['toggle']
		ctls = str(jobname) + ".ctl"
		data = str(jobname) + ".dat"
		bad = str(jobname) + ".bad"
		log = str(jobname) + ".log"

		print("Loading: " + jobname)

		if jobname in jobs and toggle == 'on':
			"""for security reasons, the full path to sqlldr has been removed"""
			sqlldr = '{fullpathtosqlldr}/sqlldr CONTROL="' + ctlsdir + '/' + ctls  + '" LOG="' + logdir + '/' + log + '" BAD="' + baddir + '/' + bad + '", DATA="' + datadir + '/' + data + '" USERID=' + userid + '/' + passw + ' ERRORS=50 SKIP=0 SILENT=FEEDBACK DIRECT=TRUE'

			subprocess.call(sqlldr, shell=True)

			message = str("{} SQLLOADED successfully!".format(jobname))
			print(message)
			status = "successful"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)

			log_cursor.execute(log_query)
			log_conn.commit()
			log_conn.close()

		else:	
			message = str("Cannot SQLLOAD {}: toggle switch is off. Loading next table.\n".format(jobname))
			print(message)
			status = "skipped"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)

			log_cursor.execute(log_query)
			log_conn.commit()
			log_conn.close()

	except KeyError:
		message = str("KeyError: " + tablename + " does not exist in this list!")
		print(message)
		status = "failed"
		schedule = jobtype

		log_conn = StatusLogger.oracleconnection()
		log_cursor = log_conn.cursor()
		log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
		print(log_query)
		log_cursor.execute(log_query)
		log_conn.commit()
		log_conn.close()

# After loading, refresh the materialized views
print("Refreshing the materialized views in fsucs_data:\n\n")

if tablename is None:
	for jobname in jobs:
		toggle = tablesconf_values[jobname]['toggle']

		if jobname in jobs and toggle == 'on':
			if jobname.startswith("cs_"):
				jobname = jobname.replace("cs_","fsuvs_")
			else:
				jobname = jobname.replace("tmp_", "fsuvs_")
			print("EXECUTE DBMS_MVIEW.REFRESH('fsucs_data." + jobname + "', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)")
			oracle_cursor.callproc('DBMS_MVIEW.REFRESH', [str("fsucs_data." + jobname), 'C'])
			
			message = str("MV {} refreshed successfully!".format(jobname))
			print(message)
			status = "successful"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
			log_update_query = StatusLogger.update_query(jobname, status)

			log_cursor.execute(log_query)
			log_cursor.execute(log_update_query)
			log_conn.commit()
			log_conn.close()
		else:
			message = str("Cannot refresh {}: toggle switch is off. Refreshing next table.\n".format(jobname))
			print(message)
			status = "skipped"
			schedule = jobtype
			jobname = jobname.replace("tmp_", "fsuvs_")

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
			log_update_query = StatusLogger.update_query(jobname, status)

			log_cursor.execute(log_query)
			log_cursor.execute(log_update_query)
			log_conn.commit()
			log_conn.close()

elif tablename is not None:
	jobname = tablename

	try:
		toggle = tablesconf_values[jobname]['toggle']

		if jobname in jobs and toggle == 'on':
			jobname = jobname.replace("tmp_", "fsuvs_")
			print("EXECUTE DBMS_MVIEW.REFRESH('fsucs_data." + jobname + "', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)")
			oracle_cursor.callproc('DBMS_MVIEW.REFRESH', [str("fsucs_data." + jobname), 'C'])
			message = str("MV {} refreshed successfully!".format(jobname))
			print(message)
			status = "successful"
			schedule = jobtype

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
			log_update_query = StatusLogger.update_query(jobname, status)

			log_cursor.execute(log_query)
			log_cursor.execute(log_update_query)
			log_conn.commit()
			log_conn.close()			
		else:
			message = str("Cannot refresh {}: toggle switch is off. Refreshing next table.\n".format(jobname))
			print(message)
			status = "skipped"
			schedule = jobtype
			jobname = jobname.replace("tmp_", "fsuvs_")

			log_conn = StatusLogger.oracleconnection()
			log_cursor = log_conn.cursor()
			log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
			log_update_query = StatusLogger.update_query(jobname, status)

			log_cursor.execute(log_query)
			log_cursor.execute(log_update_query)
			log_conn.commit()
			log_conn.close()			
	except KeyError:
		message = str("KeyError: " + tablename + " does not exist in this list!")
		print(message)
		status = "failed"
		schedule = jobtype
		jobname = jobname.replace("tmp_", "fsuvs_")

		log_conn = StatusLogger.oracleconnection()
		log_cursor = log_conn.cursor()
		log_query = StatusLogger.refresh_load_query(filename, status, schedule, jobname, message)
		log_update_query = StatusLogger.update_query(jobname, status)

		log_cursor.execute(log_query)
		log_cursor.execute(log_update_query)
		log_conn.commit()
		log_conn.close()
