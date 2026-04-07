#!/usr/bin/python3.6
import argparse
from canvasapi_logger import StatusLogger
from contextlib import contextmanager
import cx_Oracle
import datetime
from odlyaml import OdlYaml
import os
import pandas as pd
from pw_encoder_decoder import decoded
import requests
import shutil
import sys
import time

def return_session_headers():
    header = {
        'Authorization':
        'Bearer {inserttokenhere}',
        'Content-Type':
        'text/csv'
        # ,
        # 'User-Agent':
        # 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
    }
    return header

def return_session_url():
	url = "{url to the sis_imports endpoint}"

	return url

def return_post_params(datdir, filename):
	params = {
		'import_type':
		'instructure_csv',
		'extension':
		'csv'
	}

	return params

@contextmanager
def cd(newdir):
	"""
	This function will change the
	current working directory to
	the new directory, but will
	return you back to the original
	directory once the 'with' statement
	is completed.
	"""
	prevdir = os.getcwd()
	os.chdir(os.path.expanduser(newdir))
	try:
		yield
	finally:
		os.chdir(prevdir)

"""Linux-like methods to perform file handling"""
def mv(file):
	"""
	If the file exists, move it from the temp directory
	to the dat directory, else throw an exception
	"""
	#filepath has been removed for security reasons
	basedir = "{fullfilepath}/pythonCanvasAPI/"
	datdir = basedir + "dat/"
	arcdir = basedir + "arcs/"
	if os.path.isfile(file):
		currentpath = datdir + f"{file}"
		movepath = arcdir + f"{file}"
		shutil.move(currentpath, movepath)
	else:
		raise OSError(f"Filename '{file}' does not exist in directory.")

def rename(file, newfile):
	"""
	This function renames a file provided as an argument.
	"""
	if os.path.isfile(file):
		os.rename(file, newfile)
	else:
		raise OSError(f"Filename '{file}' does not exist in directory.")

def process_sis_import(datdir):
	s = requests.Session()
	url = return_session_url()
	get = s.get(url, headers=return_session_headers())

	with cd(datdir):
		files = os.listdir()
		for file in files:
			header = return_session_headers()
			params = return_post_params(datdir, file)
			print(f"Running sis import for '{file}'...")

			if get.status_code == 200:
				post = s.post(
					url,
					data=open(f"{datdir}{file}", 'rb'),
					headers=header,
					params=params
				)
				print(post.status_code)
				epoch_file = file.replace(
					".csv",
					"_" + str(int(time.time())) + ".csv")
				rename(
					file,
					epoch_file
				)
				mv(epoch_file)
				# csv.close()
				message = f"Sis_import for {file} completed successfully!"
				print(message)
				status = "successful"
				jobname = file

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()

				time.sleep(30)
			else:
				message = f"Cannot import sis file. status_code: {get.status_code}"
				print(message)
				status = "failed"
				jobname = file

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()
				sys.exit(0)


today = datetime.datetime.now()
filename = os.path.basename(__file__)
#filepath has been removed for security reasons
basedir = "{fullfilepath}/pythonCanvasAPI/"
datdir = basedir + "dat/"
confdir = basedir + "conf/"
appconf = confdir + "appconf.yaml"
canvaslistconf = confdir + "canvaslist.yaml"
canvassqlsconf = confdir + "canvas_sqls.yaml"

orglistconf = confdir + "orglist.yaml"
orgsqlsconf = confdir + "orgSqls.yaml"

appconf_yaml = OdlYaml(appconf)
appconf_values = appconf_yaml.values

"""arg-parse values"""
parser = argparse.ArgumentParser()
parser.add_argument(
	'--processorgs',
	nargs='?',
	help="Optional argument: Download the organization queries .csvs"
)
parser.add_argument(
	'--tablename',
	nargs='?',
	help="Insert table name that needs to be downloaded"
)
args = parser.parse_args()
processorgs = args.processorgs
tablename = args.tablename

if processorgs == "true":
	canvaslistconf_yaml = OdlYaml(orglistconf)
	canvaslistconf_values = canvaslistconf_yaml.values

	canvassqlsconf_yaml = OdlYaml(orgsqlsconf)
	canvassqlsconf_values = canvassqlsconf_yaml.values
elif processorgs is None:
	canvaslistconf_yaml = OdlYaml(canvaslistconf)
	canvaslistconf_values = canvaslistconf_yaml.values

	canvassqlsconf_yaml = OdlYaml(canvassqlsconf)
	canvassqlsconf_values = canvassqlsconf_yaml.values
else:
	raise ValueError(f"processorgs requires an argument of 'true'. \
	\nArgument provided: '{processorgs}'")

"""DB values"""
userid = appconf_values['userid']
passw = decoded(appconf_values['passw'])
url = appconf_values['url']
connect = cx_Oracle.connect(userid, passw, url)

tables = canvaslistconf_values['all']
terms = canvassqlsconf_values['terms']

print(f"CanvasAPI Script. Date: {today}\n")

if tablename is None:
	print("Running regular CanvasAPI")
	for table in tables:
		"""
		For each table inside of the canvas_sqls.yaml file,
		Check if the toggle switch is on, replace the varbl1
		with the numerical value of that varbl1, download a
		.csv file and place it inside of the dat/ directory.
		"""
		toggle = canvassqlsconf_values[table]['toggle']
		query = canvassqlsconf_values[table]['sql']
		varbl1 = canvassqlsconf_values[table]['varbl1']

		for number in terms:
			if number == varbl1:
				term = terms[number]

		if toggle == 'on':
			if varbl1 != "":
				query = query.replace("$varbl1", term)
			df = pd.read_sql(query, connect)
			if not df.empty:
				message = f"Creating .csv for {table}"
				print(message)
				file = open(datdir + table + ".csv", 'w+')
				file.write(
					df.to_csv(
					index=False
					)
				)
				file.close()
				status = "successful"
				jobname = table

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()

			else:
				message = f"DF for {table} is empty: No new data. Skipping {table}"
				print(message)
				status = "skipped"
				jobname = table

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()

		else:
			message = f"{table} Toggle switch off. Next table"
			print(message)
			status = "skipped"
			jobname = table

			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(filename, status, jobname, message)
			cursor.execute(query)
			conn.commit()
			conn.close()

	time.sleep(10)
	process_sis_import(datdir)

elif tablename is not None:
	print(f"Running the CanvasAPI for {tablename}")

	if tablename in tables:
		toggle = canvassqlsconf_values[tablename]['toggle']
		query = canvassqlsconf_values[tablename]['sql']
		varbl1 = canvassqlsconf_values[tablename]['varbl1']

		for number in terms:
			if number == varbl1:
				term = terms[number]

		if toggle == 'on':
			if varbl1 != "":
				query = query.replace("$varbl1", term)
			df = pd.read_sql(query, connect)
			if not df.empty:
				message = f"Creating .csv for {tablename}"
				print(message)
				file = open(datdir + tablename + ".csv", 'w+')
				file.write(
					df.to_csv(
					index=False
					)
				)
				file.close()
				status = "successful"
				jobname = tablename

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()

			else:
				message = f"DF for {tablename} is empty: No new data. Skipping {tablename}"
				print(message)
				status = "skipped"
				jobname = tablename

				conn = StatusLogger.oracleconnection()
				cursor = conn.cursor()
				query = StatusLogger.query(filename, status, jobname, message)
				cursor.execute(query)
				conn.commit()
				conn.close()
		else:
			message = f"{tablename} Toggle switch off. Next table"
			print(message)
			status = "skipped"
			jobname = tablename

			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(filename, status, jobname, message)
			cursor.execute(query)
			conn.commit()
			conn.close()

	else:
		message = f"{tablename} does not exist in the tablelist. Try again"
		print(message)
		status = "failed"
		jobname = tablename

		conn = StatusLogger.oracleconnection()
		cursor = conn.cursor()
		query = StatusLogger.query(filename, status, jobname, message)
		cursor.execute(query)
		conn.commit()
		conn.close()

	time.sleep(10)
	process_sis_import(datdir)

