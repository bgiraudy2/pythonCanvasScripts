import cx_Oracle
from odlyaml import OdlYaml
import os
import pandas as pd
from pw_encoder_decoder import encoded, decoded

class StatusLogger:
"""For security reasons, the full file path was removed."""
	def oracleconnection():
		basedir = "{fullfilepath}pythonReplicateData"
		confdir = basedir + "/conf/"
		appconf = confdir + "appconf.yaml"

		## -------- YAML VALUES -------- ##
		appconf_yaml = OdlYaml(appconf)
		appconf_values = appconf_yaml.values

		## -------- USER VALUES -------- ##
		user = appconf_values['odldata_userid']
		pw = decoded(appconf_values['odldata_passw'])
		dburl = appconf_values['odldata_url']

		connection = cx_Oracle.connect(user, pw, dburl)

		return connection

	def query(filename, status, schedule, jobname, db, message): 

		query = "INSERT INTO REPLICATE_STATUS (FILENAME, LOAD_DATE, STATUS, SCHEDULE, JOBNAME, DB, MESSAGE) VALUES ('{}', CURRENT_TIMESTAMP, '{}', '{}', '{}', '{}', '{}')".format(filename, status, schedule, jobname, db, message)

		return query

	def refresh_load_query(filename, status, schedule, jobname, message): 

		query = "INSERT INTO REPLICATE_STATUS (FILENAME, LOAD_DATE, STATUS, SCHEDULE, JOBNAME, DB, MESSAGE) VALUES ('{}', CURRENT_TIMESTAMP, '{}', '{}', '{}', 'none', '{}')".format(filename, status, schedule, jobname, message)

		return query

	def update_query(jobname, status):

		query = "UPDATE REPLICATE_CURRENT_STATUS SET STATUS = '{}' WHERE JOBNAME = '{}'".format(status, jobname)
		print(query)
		return query
