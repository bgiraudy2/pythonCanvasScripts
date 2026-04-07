import base64
import re

def encoded( password ):
	password = str.encode(password)	
	encode = base64.b64encode(password)
	encode = str(encode, 'utf-8')
	return encode

def decoded( password ):
	def base64_check( password ):
		modulo = len(password) % 4
		base64_pattern = r'[A-Za-z0-9+/]+[=]{0,2}'
		base64_regex = re.fullmatch(base64_pattern, password)
		if base64_regex and modulo == 0:
			return password
		else:
			raise ValueError("The input '{}' does not appear to be in base64".format(password))

	if base64_check( password ):
		decode = base64.b64decode(password)
		decode = str(decode, 'utf-8')

		return decode
	else:
		# this exception should not be raised... but just in case 
		raise ValueError("String was not able to be decoded")