#!/usr/bin/python3.6
import argparse
import cx_Oracle
import datetime
from odlyaml import OdlYaml
import os
import pandas as pd
from pw_encoder_decoder import encoded, decoded
from replicate_logger import StatusLogger
import sys


# the Query class is going to require an argument {all, hourly, daily, weekly} that will run the queries for that specific jobtype
# in order to run this script for the schedule type, the syntax is: 
# python3.6 downloadtables.py {all, hourly, daily, weekly}
# ex. 'python3.6 downloadtables.py hourly' downloads all of the data for the hourly process
# It is possible to run the script to download a single table, syntax for that is:
# python3.6 downloadtables.py --tablename {tmp_tablename} {all, hourly, daily, weekly}
# the {fullfilepath} is the entire path that leads to this script. for security reasons, the original path was removed.

class Query:
	statusfile = open("{fullfilepath}/pythonReplicateData/replicate_status", 'w+')
	statusfile.write("running")
	statusfile.close()
	print('status set to "running"')

	## -------- ARGPARSE -------- ##

	# The arg parse is going to pass an argument that determines the schedule type for the job.

	parser = argparse.ArgumentParser()
	parser.add_argument('jobtype', help="acceptable arguments: {all, hourly, daily, weekly, qna}")
	parser.add_argument('--tablename', nargs='?', help="Insert table name that needs to be downloaded")
	parser.add_argument('db', help="Determines which db to use in order to grab that db's data")
	args = parser.parse_args()
	
	jobtype = args.jobtype
	tablename = args.tablename
	db = args.db

	## -------- DIRECTORY VALUES -------- ##

	# Just the location of the configuration YAML files required for this script to function.

	basedir = "{fullfilepath}/pythonReplicateData"
	confdir = basedir + "/conf/"
	appconf = confdir + "appconf.yaml"
		
	## -------- YAML VALUES -------- ##

	# tablelist.yaml is just a list of the table names that will be used for running the jobs
	tablelist = OdlYaml(str(confdir) + 'tablelist.yaml')
	tablelist_values = tablelist.values

	# tablesconf.yaml is all the sql queries and tablenames for each schema, as well as all of the toggle switches for each table
	# in a yaml file

	oy = OdlYaml(str(confdir) + 'tablesconf.yaml')
	confvalues = oy.values

	# takes everything that's in the 'all' portion of the yaml and forms a list of the tablenames for the jobs.
	# options are: {all, hourly, daily, weekly}
	tablelist = tablelist_values[jobtype]

	# the total number of tables for each schedule type (as an int).
	max_val = len(tablelist)

	# gives me everything from index 0 to the last number in the list.

	jobs = tablelist[0:max_val]

	# Oracle connections info

	appconf_yaml = OdlYaml(appconf)
	appconf_values = appconf_yaml.values

	## -------- ORACLE DB INFO -------- ##

	#check teams for password
	if db == "ODL_USR" or db == "odl_usr":
		user = appconf_values['odlusr_user']
		pw = decoded(appconf_values['odlusr_pw'])
		dburl = appconf_values['odlusr_url']
	elif db == "DBPROD" or db == "dbprod":
		user = appconf_values['dbprod_user']
		pw = decoded(appconf_values['dbprod_pw'])
		dburl = appconf_values['dbprod_url']
	elif db == "DBTEST" or db == "dbtest":
		user = appconf_values['dbtest_user']
		pw = decoded(appconf_values['dbtest_pw'])
		dburl = appconf_values['dbtest_url']
	elif db == "QNA" or db == "qna":
		user = appconf_values['qna_user']
		pw = decoded(appconf_values['qna_pw'])
		dburl = appconf_values['qna_url']

	## -------- MAIN -------- ##

	filename = os.path.basename(__file__)

	# for the following block of code, the script will attempt to connect to the schema
	# If there is an invalid user/pass (1017), no password was given (1005)
	# or if the URL cannot connect for whatever reason (12162),
	# Exit the program with a DatabaseError exception

	try:
		oracle_connection = cx_Oracle.connect(user, pw, dburl)
	except cx_Oracle.DatabaseError as e:
		errorObj, = e.args
		print("Error code: " + str(errorObj.code))
		print(errorObj.message)

		if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
			print("Exiting program now")
			sys.exit()

	oracle_cursor = oracle_connection.cursor()


	# for each job, it's going to check and see if the 'toggle' switch is set to on,
	# if the switch is on, it's going to place the results in the query variable and 
	# replace all of the $varbl1 variables inside of the queries the yaml file with the actual
	# varbl1 that's located inside of the tablesconf.yaml file

	# If a tablename is not specified as an argument for the script, it will just run all of the jobs
	# As per the tablelist schedule list.
	# It will also update the replicate_status table indicating whether or not the file download
	# was successful. If it's not, it will update the table to indicate so.

	# Otherwise if a tablename is specified, it will download only that table and update the replicate_status table.

	terms = confvalues['terms']

	if tablename is None:
		for jobname in jobs:
			toggle = confvalues[jobname]['toggle']
			varbl1 = confvalues[jobname]['varbl1']
		
			for keys in terms:
				if keys == varbl1:
					term = terms[keys]

			if jobname in jobs and toggle == "on":
				query = confvalues[jobname]['sql']

				if varbl1 != "":
					query = query.replace("$varbl1", term)
				print("Downloading " + jobname)
				oracle_query = query
				oracle_pd = pd.read_sql(oracle_query, con=oracle_connection)
				oracle_pd.columns = oracle_pd.columns.str.lower()
				file = open("dat/" + str(jobname) + ".dat", "w+")
				file.write(oracle_pd.to_csv(
					index=False, 
					header=False, 
					sep='|', 
					line_terminator="#EOR#\n")
				)
				message = str(jobname + ".dat downloaded.\n")
				print(message)
				file.close()
				#log the outcome of the operation above:
				status = "successful"
				schedule = jobtype

				try:
					log_conn = StatusLogger.oracleconnection()
				except cx_Oracle.DatabaseError as e:
					errorObj, = e.args
					print("Error code: " + str(errorObj.code))
					print(errorObj.message)

					if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
						print("Exiting program now")
						sys.exit()

				log_cursor = log_conn.cursor()
				log_query = StatusLogger.query(filename, status, schedule, jobname, db, message)
				
				log_cursor.execute(log_query)
				log_conn.commit()
				log_conn.close()

			else:
				message = str("cannot download {} data: toggle switch is off. Loading next table.\n".format(jobname))
				print(message)
				status = "skipped"
				schedule = jobtype

				try:
					log_conn = StatusLogger.oracleconnection()
				except cx_Oracle.DatabaseError as e:
					errorObj, = e.args
					print("Error code: " + str(errorObj.code))
					print(errorObj.message)

					if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
						print("Exiting program now")
						sys.exit()

				log_cursor = log_conn.cursor()
				log_query = StatusLogger.query(filename, status, schedule, jobname, db, message)
				
				log_cursor.execute(log_query)
				log_conn.commit()
				log_conn.close()

	elif tablename is not None:
		jobname = tablename

		try:
			toggle = confvalues[jobname]['toggle']
			varbl1 = confvalues[jobname]['varbl1']

			for keys in terms:
				if keys == varbl1:
					term = terms[keys]

			if toggle == "on":
				query = confvalues[jobname]['sql']

				if varbl1 != "":
					query = query.replace("$varbl1", term)
				print("Downloading " + jobname)
				oracle_query = query
				oracle_pd = pd.read_sql(oracle_query, con=oracle_connection)
				oracle_pd.columns = oracle_pd.columns.str.lower()
				file = open("dat/" + str(jobname) + ".dat", "w+")
				file.write(oracle_pd.to_csv(
					index=False, 
					header=False, 
					sep='|', 
					line_terminator="#EOR#\n")
				)
				message = str(jobname + ".dat downloaded.\n")
				print(message)
				file.close()
				#log the outcome of the operation above:
				status = "successful"
				schedule = jobtype

				try:
					log_conn = StatusLogger.oracleconnection()
				except cx_Oracle.DatabaseError as e:
					errorObj, = e.args
					print("Error code: " + str(errorObj.code))
					print(errorObj.message)

					if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
						print("Exiting program now")
						sys.exit()

				log_cursor = log_conn.cursor()
				log_query = StatusLogger.query(
					filename, 
					status, 
					schedule, 
					jobname, 
					db, 
					message
				)
				log_cursor.execute(log_query)
				
				log_conn.commit()
				log_conn.close()
			else:
				message = str("cannot download {} data: toggle switch is off. Loading next table.\n".format(jobname))
				print(message)
				status = "skipped"
				schedule = jobtype

				try:
					log_conn = StatusLogger.oracleconnection()
				except cx_Oracle.DatabaseError as e:
					errorObj, = e.args
					print("Error code: " + str(errorObj.code))
					print(errorObj.message)

					if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
						print("Exiting program now")
						sys.exit()

				log_cursor = log_conn.cursor()
				log_query = StatusLogger.query(
					filename, 
					status, 
					schedule, 
					jobname, 
					db, 
					message
				)
				log_cursor.execute(log_query)
				log_conn.commit()
				log_conn.close()

		except KeyError:
			message = str("KeyError: " + tablename + " does not exist in this list!")
			print(message)
			status = "failed"
			schedule = jobtype

			try:
				log_conn = StatusLogger.oracleconnection()
			except cx_Oracle.DatabaseError as e:
				errorObj, = e.args
				print("Error code: " + str(errorObj.code))
				print(errorObj.message)

				if errorObj.code == 1017 or errorObj.code == 1005 or errorObj.code == 12162:
					print("Exiting program now")
					sys.exit()

			log_cursor = log_conn.cursor()
			log_query = StatusLogger.query(
				filename, 
				status, 
				schedule, 
				jobname, 
				db, 
				message
			)
			print(log_query)
			log_cursor.execute(log_query)
			log_conn.commit()
			log_conn.close()

Query
