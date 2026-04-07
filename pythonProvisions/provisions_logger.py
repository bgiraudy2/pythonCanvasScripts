import cx_Oracle
from odlyaml import OdlYaml
from pw_encoder_decoder import encoded, decoded

class StatusLogger:

	def oracleconnection():
		"""For security reasons, the full file path has been removed"""
		basedir = "{fullfilepath}/pythonProvisions"
		confdir = basedir + "/conf/"
		appconf = confdir + "appconf.yaml"

		## -------- YAML VALUES -------- ##
		appconf_yaml = OdlYaml(appconf)
		appconf_values = appconf_yaml.values

		## -------- USER VALUES -------- ##
		userid = appconf_values['userid']
		passw = decoded(appconf_values['passw'])
		url = appconf_values['url']

		connection = cx_Oracle.connect(userid, passw, url)

		return connection

	def query(filename, status, jobname, message): 

		query = f"INSERT INTO PROVISIONS_STATUS (FILENAME, LOAD_DATE, STATUS, JOBNAME, MESSAGE) VALUES ('{filename}', CURRENT_TIMESTAMP, '{status}', '{jobname}', '{message}')"

		return query

	def update_query(jobname, status):

		query = f"UPDATE PROVISIONS_CURRENT_STATUS SET STATUS = '{status}' WHERE JOBNAME = '{jobname}'"
		print(query)
		return query
