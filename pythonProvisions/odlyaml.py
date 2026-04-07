import odlutils
from ruamel.yaml import YAML

class OdlYaml: 
	def __init__( self, path ):
	## check if the file exists, if it exists; load yaml values into var. else don't do anything
		if path[0] != "/":
			path = odlutils.getlocation() + "/" + path

		if odlutils.rcheck( path ):
			file = odlutils.readfile( path )

			yaml = YAML(typ='safe')
			values = yaml.load( file )

			self.values = values
		else:
			print( "File does not exist: " + path )

		#determine if value that was given (path), is it full path or relative path
		# relative path does not start with a slash
		#full path is the entire dir

		#if path doesn't start with a slash, turn it into full path

			#sample.yaml
			#then get location and add the slash

		# if given full path, do nothing no else, check if it's valid

# oy is short for the odlyaml object

# oy = OdlYaml('config/bad.yaml')

# yamlvalues = oy.values
# print(yamlvalues)

# get value out of YAML file