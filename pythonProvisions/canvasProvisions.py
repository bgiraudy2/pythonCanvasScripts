#!/usr/bin/python3.6
import argparse
from contextlib import contextmanager
import cx_Oracle
import datetime
import io
from odlyaml import OdlYaml
import os
import pandas as pd
from pw_encoder_decoder import decoded
from provisions_logger import StatusLogger
import regex
import requests
import shutil
import subprocess
import sys
import time
from zipfile import ZipFile

class CanvasSession:
	"""For security reasons, the full file path has been removed"""
	basedir = "{fullfilepath}/pythonProvisions/"
	tempdir = basedir + "temp/"
	def __init__(self, session):
		self.session = session

	def return_session_url(self):
		url = "{the URL to Canvas' provisioning_csv endpoint}"

		return url

	def return_session_headers(self):
		header = {
			'Authorization':
			'Bearer {inserttoken}',
			'User-Agent':
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'
		}
		return header

	"""non_term_stuff"""
	def process_nonterm_csv(self, session):
		"""
		Returns a status code of 200 if you are able
		to connect to Canvas using a session object.
		"""
		url = self.return_session_url()
		header = self.return_session_headers()
		params = {
			'parameters[users]': 1,
			'parameters[accounts]': 1,
			'parameters[terms]': 1
		}
		"""
		This post request is going to kick-start the
		provisioning report in Canvas Test.
		"""
		nonterm_post = session.post(
			url,
			headers=header,
			params=params
		)
		print("Non-term status code: " + str(nonterm_post.status_code))
		"""
		This get request is going to retrieve the json
		that contains the URLs to download the provisioning report.
		"""
		get = session.get(
			url,
			headers=header
		)

		return get

	def download_nonterm_csv(self, zip_url, session):
		"""
		This function will download the non-term zip
		from the download url returned back during the
		get_json() function
		"""
		today = datetime.date.today()
		header = self.return_session_headers()
		r = session.get(
			zip_url,
			headers=header
		)

		if r.status_code == 200:
			file = open(
				self.tempdir + f"nonterm_provisioning_{today}.zip",
				'wb').write(r.content)
			print("Zip file downloaded.")
		else:
			print("Cannot download zip file: status code != 200")

	def get_nonterm_json(self, get, session):
		"""
		This function takes the json response from the
		get request and returns a pandas dataframe.
		The progress should be 0 and file_url should be null
		since you just ran the report.

		If you get a status code of 200, add the json response
		to a pandas dataframe and retrieve the progress and file_url
		"""
		url = self.return_session_url()
		header = self.return_session_headers()
		if get.status_code == 200:
			json = get.json()
			jpd = pd.DataFrame(json)
			progress = jpd['progress'][0]
			file_url = jpd['file_url'][0]
			provstatus = jpd['status'][0]

			if progress == 0 and file_url is None:
				"""
				Wait one minute
				If the progress of the report is 100 and file_url
				is not 'None', download the report as a .csv and
				place it in the dat directory
				Else,
				Wait another minute, and try downloading the csv again
				"""
				print("Downloading the .csv in one minute...")
				time.sleep(60)
				newresponse = session.get(
					url,
					headers=header
				)
				self.get_nonterm_json(newresponse, session)
			else:
				if provstatus == 'error':
					errormessage = f"Canvas Provisioning received an error of: '{provstatus}' " \
					"Cancelling Provisions and the job entirely"
					print(errormessage)
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("failed")
					statusfile.close()
					sys.exit(0)
				else:
					print("Downloading now!!!")
					zip_url = jpd['attachment'][0]['url']
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("successful")
					statusfile.close()

				self.download_nonterm_csv(
					zip_url, session
					)
		else:
			print("Status code returned something other than 200.")
			print("Exiting.")
			sys.exit()

	"""term_stuff"""
	def process_enrollment_terms(self, session):
		"""
		This function will download the term zip
		based on the pagination of the terms url.

		It will need to go through each page
		in the header and see what the 'next'
		page is and if the 'last' page is null,
		the cycle is complete.
		"""
		url = "{url to Canvas' terms endpoint}"
		header = self.return_session_headers()

		get = session.get(url, headers=header)

		"""Get the header, give me the links, and do stuff with it"""
		head = get.headers
		pages = requests.utils.parse_header_links(
			head['link'].rstrip('>').replace('>,<', ',<')
		)
		page_pd = pd.DataFrame(pages)

		rowcount = page_pd.shape[0]

		"""Gives me the url of the first and last page"""
		for row in range(rowcount):
			if page_pd['rel'][row] == 'first':
				first = page_pd['url'][row]
			if page_pd['rel'][row] == 'current':
				current = page_pd['url'][row]
			if page_pd['rel'][row] == 'prev':
				prev = page_pd['url'][row]
			if page_pd['rel'][row] == 'next':
				nxt = page_pd['url'][row]
			if page_pd['rel'][row] == 'last':
				last = page_pd['url'][row]

		m = regex.search('[?&]page=\K[0-9]*', last)
		last_page = int(m.group(0))
		df = pd.DataFrame()

		for i in range(1, last_page + 1):
			format_url = f"https://{url}?page={i}"
			s = session.get(format_url, headers=header)
			content = s.json()
			df = df.append(content, ignore_index=True)

		return df

	def retrieve_term_ids(self, df):
		"""
		This function is going to go through
		the dataframe created by download_term_csv()
		to locate the numerical term id based on the
		verbal 'sis_term_id'.
		"""
		#the terms value is going to be placed in a YAML file
		basedir = "{fullfilepath}/"
		confdir = basedir + "conf/"
		tablesconf = confdir + "tablesconf.yaml"
		tablesconf_yaml = OdlYaml(tablesconf)
		tablesconf_values = tablesconf_yaml.values
		terms = tablesconf_values['terms']

		term_ids = []
		for page in range(df.shape[0]):
			response = df.enrollment_terms[page]
			limit = len(response)
			for i in range(limit):
				for term in terms:
					if response[i]['sis_term_id'].startswith(term):
						term_ids.append(response[i]['id'])
		return term_ids

	def get_term_json(self, term_id, term_prov_url, term_get, session):
		today = datetime.date.today()
		header = self.return_session_headers()
		if term_get.status_code == 200:
			term_json = term_get.json()
			termjpd = pd.DataFrame(term_json)
			term_prog = termjpd['progress'][0]
			term_furl = termjpd['file_url'][0]
			term_provstatus = termjpd['status'][0]

			if term_prog == 0 and term_furl is None:
				print("Downloading the terms .csvs in one minute...")
				time.sleep(60)
				termresponse = session.get(
					term_prov_url,
					headers=header
				)
				self.get_term_json(
					term_id,
					term_prov_url,
					termresponse,
					session
				)
			else:
				if term_provstatus == 'error':
					errormessage = f"Canvas Provisioning received an error of: '{term_provstatus}' " \
					"Cancelling Provisions and the job entirely"
					print(errormessage)
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("failed")
					statusfile.close()
					sys.exit(0)
				else:
					print(f"Download term {term_id} .csv now!!!")
					term_zip_url = termjpd['attachment'][0]['url']
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("successful")
					statusfile.close()

				term_r = session.get(
					term_zip_url,
					headers=header
				)

				if term_r.status_code == 200:
					file = open(self.tempdir +
						f"term_provisioning_{term_id}_{today}.zip",
						 'wb').write(term_r.content)
					print(f"Zip for term {term_id} downloaded.")
				else:
					print(f"Cannot download term {term_id} zip file: \
					status code != 200")

	def get_hist_json(self, hist_prov_url, hist_get, session):
		today = datetime.date.today()
		header = self.return_session_headers()
		if hist_get.status_code == 200:
			hist_json = hist_get.json()
			histjpd = pd.DataFrame(hist_json)
			hist_prog = histjpd['progress'][0]
			hist_furl = histjpd['file_url'][0]
			hist_provstatus = histjpd['status'][0]

			if hist_prog == 0 and hist_furl is None:
				print("Downloading the hist .csvs in one minute...")
				time.sleep(60)
				histresponse = session.get(
					hist_prov_url,
					headers=header
				)
				self.get_hist_json(
					hist_prov_url,
					histresponse,
					session
				)
			else:
				if hist_provstatus == 'error':
					errormessage = f"Canvas Provisioning received an error of: '{hist_provstatus}' " \
					"Cancelling Provisions and the job entirely"
					print(errormessage)
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("failed")
					statusfile.close()
					sys.exit(0)
				else:
					print(f"Downloading historical .csv now!!!")
					hist_zip_url = histjpd['attachment'][0]['url']
					statusfile = open("{fullfilepath}/provisions_status", 'w+')
					statusfile.write("successful")
					statusfile.close()

				hist_r = session.get(
					hist_zip_url,
					headers=header
				)

				if hist_r.status_code == 200:
					file = open(self.tempdir +
						f"term_provisioning_hist_{today}.zip",
						 'wb').write(hist_r.content)
					print(f"Zip for historical downloaded.")
				else:
					print(f"Cannot download historical zip file: \
					status code != 200")

	def process_term_csv(self, term_ids, session):
		"""
		This function takes the term_ids list,
		and will send a request to download the report for each
		term id, and place it in the "temp/" directory.
		"""
		term_prov_url = "{url to Canvas' provisioning_csv endpoint}"
		header = self.return_session_headers()

		for term_id in term_ids:
			term_prov_params = {
				'parameters[enrollment_term_id]': term_id,
				'parameters[courses]': 1,
				'parameters[sections]': 1,
				'parameters[enrollments]': 1,
				'parameters[xlist]': 1,
				'parameters[include_deleted]': 1,
			}
			term_post = session.post(
				term_prov_url,
				headers=header,
				params=term_prov_params
			)
			print(f"Term {term_id} status code: "
				+ str(term_post.status_code)
			)
			term_get = session.get(
				term_prov_url,
				headers=header
			)
			self.get_term_json(
				term_id,
				term_prov_url,
				term_get,
				session
			)

		print("Term provisioning completed!")

	"""Linux-like methods to perform file handling"""
	def mv(self, file):
		"""
		If the file exists, move it from the temp directory
		to the dat directory, else throw an exception
		"""
		basedir = "{fullfilepath}/"
		tempdir = basedir + "temp/"
		datdir = basedir + "dat/"
		if os.path.isfile(file):
			currentpath = tempdir + f"{file}"
			movepath = datdir + f"{file}"
			shutil.move(currentpath, movepath)
		else:
			raise OSError(f"Filename '{file}' does not exist in directory.")

	def rm(self, file):
		"""
		This function deletes the specified file from the
		temp directory.
		If the file does exist, delete it, else, throw an exception.
		"""
		if os.path.isfile(file):
			os.remove(file)
		else:
			raise OSError(f"Filename '{file}' does not exist in directory.")

	def rmvdir(self, directory):
		if os.path.isdir(directory):
			shutil.rmtree(directory)
		else:
			raise OSError(f"Directory '{directory}' does not exist in directory.")

	def rename(self, file, newfile):
		"""
		This function renames a file provided as an argument.
		"""
		if os.path.isfile(file):
			os.rename(file, newfile)
		else:
			raise OSError(f"Filename '{file}' does not exist in directory.")

	"""Once you have all of the zips downloaded, it's time to unzip them"""
	def unzip_csvs(self, process):
		"""
		This function is going to unzip each of the zip
		files resulting from the output of the
		nonterm/term functions.
		The basedir will need to be changed, or added to a YAML,
		or both?

		process could be {nonterm|term}

		Specifying the process will unzip the files based on that
		process type
		"""
		basedir = "{fullfilepath}/"
		tempdir = basedir + "temp/"
		datdir = basedir + "dat/"
		arcdir = basedir + "logs/arcs/"

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

		with cd(tempdir):
			files = os.listdir()
			print(files)

			for file in files:
				if process == "nonterm" and file.startswith("nonterm_"):
					# print(os.getcwd())
					z = ZipFile(file, mode='r')
					z.extractall()
					self.rename(
						'users.csv',
						'tmp_cnv_users.csv'
					)
					self.rename(
						'terms.csv',
						'tmp_cnv_terms.csv'
					)
					self.rename(
						'accounts.csv',
						'tmp_cnv_accounts.csv'
					)
					self.rm(file)
					self.mv('tmp_cnv_users.csv')
					self.mv('tmp_cnv_terms.csv')
					self.mv('tmp_cnv_accounts.csv')
					print("Non-term csvs unzipped and csvs placed in 'dat/' directory.")
				elif process == "term" and file.startswith("term_"):
					#unzip the term stuff
					term_id = regex.findall(
						"^[a-z]{4}_[a-z]{12}_([a-z0-9]+)_[0-9]{4}-[0-9]{2}-[0-9]{2}\.[a-z]{3}$",
						file
					)
					term_id = term_id[0]
					z = ZipFile(file, mode='r')
					z.extractall(str(term_id))
					self.rm(file)
					print(f"Term {term_id} csvs unzipped and csvs placed in 'temp/' directory.")
				elif process == "hist" and file.startswith("term_"):
					z = ZipFile(file, mode='r')
					z.extractall()
					self.rename(
						'courses.csv',
						'tmp_cnv_courses_hist.csv'
					)
					self.rename(
						'enrollments.csv',
						'tmp_cnv_enrollments_hist.csv'
					)
					self.rename(
						'sections.csv',
						'tmp_cnv_sections_hist.csv'
					)
					self.rename(
						'xlist.csv',
						'tmp_cnv_xlist_hist.csv'
					)
					self.rm(file)
					self.mv('tmp_cnv_courses_hist.csv')
					self.mv('tmp_cnv_enrollments_hist.csv')
					self.mv('tmp_cnv_sections_hist.csv')
					self.mv('tmp_cnv_xlist_hist.csv')
				else:
					raise ValueError("Process type not provided. " +
						"Please enter 'nonterm' or 'term'")

	def mv_term_csvs(self):
		"""
		This function will move the term_id files and place them
		inside of the dat/ directory
		"""
		basedir = "{fullfilepath}/"
		tempdir = basedir + "temp/"
		datdir = basedir + "dat/"

		print("Moving term csvs from temp/ to dat/")

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
		#Go inside of the temp directory
		with cd(tempdir):
			files = os.listdir()

			for file in files:
				termwd = tempdir + file + "/"
				#go inside of each term directory
				with cd(termwd):
					termfiles = os.listdir()
					# print(str(file) + ": " +  str(termfiles))

					for termfile in termfiles:
						# print(termfile)
						fsrc = open(termfile, 'r')
						fdst = open(
							"../" +
							'tmp_cnv_' +
							str(termfile),
							'a+'
						)
						shutil.copyfileobj(fsrc, fdst)
						fsrc.close()
						fdst.close()

						# self.mv()
			currfiles = os.listdir()
			for currfile in currfiles:
				if currfile.startswith("tmp_cnv_"):
					self.mv(currfile)

		with cd(tempdir):
			lastfiles = os.listdir()
			for lastfile in lastfiles:
				self.rmvdir(lastfile)

	def mv_org_csvs(self):
		"""
		This function will move only the org term csvs and place them
		inside of the dat/ directory.
		"""
		basedir = "{fullfilepath}/"
		tempdir = basedir + "temp/"
		datdir = basedir + "dat/"

		print("Moving org csvs from temp/ to dat/")

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

		with cd(tempdir):
			files = os.listdir()

			for file in files:
				orgwd = tempdir + file + "/"
				with cd(orgwd):
					orgfiles = os.listdir()
					for orgfile in orgfiles:
						fsrc = open(orgfile, 'r')
						fdst = open(
							"../" +
							'tmp_cnv_org_' +
							str(orgfile),
							'w+'
						)
						shutil.copyfileobj(fsrc, fdst)
						fsrc.close()
						fdst.close()
			currfiles = os.listdir()
			for currfile in currfiles:
				if currfile.startswith("tmp_cnv_org_"):
					self.mv(currfile)
		with cd(tempdir):
			lastfiles = os.listdir()
			for lastfile in lastfiles:
				self.rmvdir(lastfile)

	def rename_org_csvs(self):
		"""
		This function will rename the org files
		so that the SQLLOADer ctl file can load it
		since the renamed file name should be the same
		as the tmp_table name
		"""
		basedir = "{fullfilepath}/"
		datdir = basedir + "dat/"
		print("Renaming the org .csvs now.")

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

		with cd(datdir):
			files = os.listdir()
			for file in files:
				if file == 'tmp_cnv_org_courses.csv':
					self.rename(
						'tmp_cnv_org_courses.csv',
						'tmp_cnv_orgs.csv'
					)
				elif file == 'tmp_cnv_org_enrollments.csv':
					self.rename(
						'tmp_cnv_org_enrollments.csv',
						'tmp_cnv_org_enroll.csv'
					)
				else:
					pass

	def process_org_csv(self, term, session):
		"""
		This function downloads only the org term, which contains
		all of the org-based provision reports. It then places
		these files in the temp/ directory.
		"""
		org_prov_url = "{url to Canvas' provisioning_csv endpoint}"
		header = self.return_session_headers()
		org_prov_params = {
			'parameters[enrollment_term_id]': term,
			'parameters[courses]': 1,
			'parameters[enrollments]': 1,
			'parameters[include_deleted]': 1,
		}
		org_post = session.post(
			org_prov_url,
			headers=header,
			params=org_prov_params
		)
		print(f"{term} term-based status code: "
				+ str(org_post.status_code)
		)
		org_get = session.get(
			org_prov_url,
			headers=header
		)
		self.get_term_json(
			term,
			org_prov_url,
			org_get,
			session
		)

		print("Org provisioning completed!")

	def process_hist_csv(self, session):
		"""
		This function takes the term_ids list,
		and will send a request to download the report for each
		term id, and place it in the "temp/" directory.
		"""
		hist_prov_url = "{url to Canvas' provisioning_csv endpoint}"
		header = self.return_session_headers()


		hist_prov_params = {
			'parameters[courses]': 1,
			'parameters[sections]': 1,
			'parameters[enrollments]': 1,
			'parameters[xlist]': 1,
			'parameters[include_deleted]': 1,
		}
		hist_post = session.post(
			hist_prov_url,
			headers=header,
			params=hist_prov_params
		)
		print(f"Historical status code: "
			+ str(hist_post.status_code)
		)
		hist_get = session.get(
			hist_prov_url,
			headers=header
		)
		self.get_hist_json(
			hist_prov_url,
			hist_get,
			session
		)

		print("Historical provisioning completed!")

	def create_dats(self, file):
		"""
		This function will take the csvs,
		remove the header, rename the file to .dat,
		and then deletes the csvs.
		"""
		basedir = "{fullfilepath}/"
		datdir = basedir + "dat/"

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

		with cd(datdir):
			files = os.listdir()

			if file in files and os.path.isfile(file):
				print(file)
				df = pd.read_csv(file)
				df.drop_duplicates(keep='last')
				datfile = open(file, "w+")
				datfile.write(df.to_csv(
					index=False,
					header=False,
					sep='|')
				)
			else:
				message = f"File '{file}' does not exist!"
				raise OSError(message)

	def wipe_temp(self):
			basedir = "{fullfilepath}/"
			tempdir = basedir + "temp"

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

			with cd(tempdir):
				temp = os.listdir()
				#print(tempdir)
				#print(os.getcwd())

				#First, check if you are in the temp directory
				#If you are
					#print temp directory location
					#Print then delete every file that's in the temp directory
					#Let the user know when it starts and ends

				if temp == [] and len(temp) == 0:
					print("Temp directory is currently empty!")
					pass
				else:
					if os.getcwd() == tempdir:
						print(str(os.getcwd) + "= temp directory TRUE")
						for file in temp:
							# If it's a directory, use the rmvdir() function to delete it
							if os.path.isdir(file):
								self.rmvdir(file)
							# else, use the rm() function to delete it
							elif os.path.isfile(file):
								self.rm(file)
							else:
								print(f"File ''{file}'' couldn't be deleted")
						print("Files in temp directory deleted!")
					else:
						print("You are not in the temp directory")
						print(os.getcwd())
						pass


class Sqlldr():
	"""
	Instantiating the CanvasSesssion class without using
	inheritance
	"""
	"""directory values"""
	filename = os.path.basename(__file__)
	basedir = "{fullfilepath}/"
	tempdir = basedir + "temp/"
	datdir = basedir + "dat/"
	baddir = basedir + "bad/"
	ctlsdir = basedir + "ctls/"
	confdir = basedir + "conf/"
	logdir = basedir + "logs/"

	appconf = confdir + "appconf.yaml"
	tablelistconf = confdir + "tablelist.yaml"
	tablesconf = confdir + "tablesconf.yaml"

	"""YAML Values"""
	tablelist = OdlYaml(tablelistconf)
	tablelist_values = tablelist.values
	tables = tablelist_values['all']

	appconf_yaml = OdlYaml(appconf)
	appconf_values = appconf_yaml.values

	tablesconf_yaml = OdlYaml(tablesconf)
	tablesconf_values = tablesconf_yaml.values

	"""DB/SQLLDR values"""
	userid = appconf_values['userid']
	passw = decoded(appconf_values['passw'])
	url = appconf_values['url']

	"""cx_oracle values"""
	connect = cx_Oracle.connect(userid, passw, url)
	cursor = connect.cursor()

	def load(self, table):
		toggle = self.tablesconf_values[table]['toggle']
		ctls = str(table) + ".ctl"
		data = str(table) + ".csv"
		bad = str(table) + ".bad"
		log = str(table) + ".log"
		datafile = self.datdir + data
		jobname = table

		sqlldr = '{fullpathtosqlldr}/sqlldr CONTROL="' + self.ctlsdir + ctls  + '" LOG="' + self.logdir + log + '" BAD="' + self.baddir + bad + '", DATA="' + self.datdir + data + '" USERID=' + self.userid + '/' + self.passw + ' ERRORS=50 SKIP=0 SILENT=FEEDBACK DIRECT=TRUE'

		if table in self.tables and toggle == "on":
			print(f"Loading {table}:\n")
			print(sqlldr)
			subprocess.call(sqlldr, shell=True)
			status = "successful"
			message = f"{jobname} SQLLOADED successfully!"
			print(message)
			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(self.filename, status, jobname, message)
			cursor.execute(query)
			conn.commit()
			conn.close()
		else:
			message = f"Table not loaded: {jobname} toggle = 'off'."
			raise ValueError(message)
			status = "skipped"
			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(self.filename, status, jobname, message)
			cursor.execute(query)
			conn.commit()
			conn.close()

	def refresh(self, table):
		toggle = self.tablesconf_values[table]['toggle']

		if table in self.tables and toggle == "on":
			table = table.replace('tmp_cnv_', 'canvas_')
			print(f"Refreshing {table}:\n")
			print(f"EXECUTE DBMS_MVIEW.REFRESH('{table}', method => 'C', atomic_refresh => FALSE, out_of_place => TRUE)")
			self.cursor.callproc('DBMS_MVIEW.REFRESH', [f"{table}", 'C'])
			message = f"{table} refreshed successfully."
			print(message)
			jobname = table
			status = "successful"
			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(self.filename, status, jobname, message)
			update = StatusLogger.update_query(jobname, status)
			cursor.execute(query)
			cursor.execute(update)
			conn.commit()
			conn.close()
		else:
			table = table.replace('tmp_cnv_', 'canvas_')
			message = f"Table not refreshed: {table}  does not exist and/or toggle = 'off'."
			raise ValueError(message)
			status = "skipped"
			jobname = table
			conn = StatusLogger.oracleconnection()
			cursor = conn.cursor()
			query = StatusLogger.query(self.filename, status, jobname, message)
			update = StatusLogger.update_query(jobname, status)
			cursor.execute(query)
			cursor.execute(update)
			conn.commit()
			conn.close()

def main():
	s = requests.Session()
	cs = CanvasSession(s)
	sql = Sqlldr()

	parser = argparse.ArgumentParser()
	parser.add_argument('--processorgs',
		nargs='?',
		help="Optional argument: Download org-based provisions"
	)
	parser.add_argument('--historical',
		nargs='?',
		help="Optional argument: Download all the terms in the provision reports"
	)
	args = parser.parse_args()
	processorgs = args.processorgs
	historical = args.historical
	today = datetime.datetime.now()
	print(f"Canvas Provisioning Script. Date: {today}")

	if processorgs == "true":
		print("Beginning org-based provisioning")
		cs.wipe_temp()
		cs.process_org_csv('44', s)
		cs.unzip_csvs("term")
		cs.mv_org_csvs()
		cs.create_dats("tmp_cnv_org_courses.csv")
		cs.create_dats("tmp_cnv_org_enrollments.csv")
		cs.rename_org_csvs()
		sql.load("tmp_cnv_orgs")
		sql.load("tmp_cnv_org_enroll")
		sql.refresh("tmp_cnv_orgs")
		sql.refresh("tmp_cnv_org_enroll")
	elif processorgs is None and historical is None:
		# """process the nonterm data"""
		print("Beginning non-term provisioning: ")
		cs.wipe_temp()
		get = cs.process_nonterm_csv(s)
		cs.get_nonterm_json(get, s)
		cs.unzip_csvs("nonterm")

		"""process the term data"""
		print("Beginning term-based provisioning: ")
		df = cs.process_enrollment_terms(s)
		term_ids = cs.retrieve_term_ids(df)
		cs.process_term_csv(term_ids, s)
		cs.unzip_csvs("term")
		cs.mv_term_csvs()
		cs.create_dats("tmp_cnv_accounts.csv")
		cs.create_dats("tmp_cnv_courses.csv")
		cs.create_dats("tmp_cnv_enrollments.csv")
		cs.create_dats("tmp_cnv_sections.csv")
		cs.create_dats("tmp_cnv_terms.csv")
		cs.create_dats("tmp_cnv_users.csv")
		cs.create_dats("tmp_cnv_xlist.csv")

		"""sqlload all of the csvs"""
		sql.load("tmp_cnv_accounts")
		sql.load("tmp_cnv_courses")
		sql.load("tmp_cnv_enrollments")
		sql.load("tmp_cnv_sections")
		sql.load("tmp_cnv_terms")
		sql.load("tmp_cnv_users")
		sql.load("tmp_cnv_xlist")

		"""refresh all of the mat views"""
		sql.refresh("tmp_cnv_accounts")
		sql.refresh("tmp_cnv_courses")
		sql.refresh("tmp_cnv_enrollments")
		sql.refresh("tmp_cnv_sections")
		sql.refresh("tmp_cnv_terms")
		sql.refresh("tmp_cnv_users")
		sql.refresh("tmp_cnv_xlist")
	elif historical == "true" and processorgs is None:
		print("Beginning historical provisioning report: ")
		cs.wipe_temp()
		cs.process_hist_csv(s)
		cs.unzip_csvs("hist")
		cs.create_dats("tmp_cnv_courses_hist.csv")
		cs.create_dats("tmp_cnv_enrollments_hist.csv")
		cs.create_dats("tmp_cnv_sections_hist.csv")
		cs.create_dats("tmp_cnv_xlist_hist.csv")

		"""sqlload all of the historical csvs"""
		sql.load("tmp_cnv_courses_hist")
		sql.load("tmp_cnv_enrollments_hist")
		sql.load("tmp_cnv_sections_hist")
		sql.load("tmp_cnv_xlist_hist")

		"""refresh all of the hist mat views"""
		sql.refresh("tmp_cnv_courses_hist")
		sql.refresh("tmp_cnv_enrollments_hist")
		sql.refresh("tmp_cnv_sections_hist")
		sql.refresh("tmp_cnv_xlist_hist")
	else:
		raise ValueError(f"processorgs or historical requires an argument of 'true'. \
		\nArgument provided: '{processorgs}'")

if __name__ == "__main__":
	main()
