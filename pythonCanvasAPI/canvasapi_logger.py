import cx_Oracle
from odlyaml import OdlYaml
from pw_encoder_decoder import encoded, decoded

class StatusLogger:

	def oracleconnection():
		"""The full file path has been removed for security reasons"""
		basedir = "{fullfilepath}/pythonCanvasAPI"
		confdir = basedir + "/conf/"
		appconf = confdir + "appconf.yaml"

		## -------- YAML VALUES -------- ##
		appconf_yaml = OdlYaml(appconf)
		appconf_values = appconf_yaml.values

		## -------- USER VALUES -------- ##
		odldata_userid = appconf_values['odldata_userid']
		odldata_passw = decoded(appconf_values['odldata_passw'])
		odldata_url = appconf_values['odldata_url']

		connection = cx_Oracle.connect(odldata_userid, odldata_passw, odldata_url)

		return connection

	def query(filename, status, jobname, message):

		query = f"INSERT INTO CANVASAPI_STATUS (FILENAME, LOAD_DATE, STATUS, JOBNAME, MESSAGE) VALUES ('{filename}', CURRENT_TIMESTAMP, '{status}', '{jobname}', '{message}')"

		return query
