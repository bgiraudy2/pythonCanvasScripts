import base64
import argparse
from pw_encoder_decoder import encoded, decoded

# this script will either encode or decode a string depending on which method you choose

# encoder_decoder.py encode/decode "string"

def arg_parse( ):
	parser = argparse.ArgumentParser()

	parser.add_argument("method_type")
	parser.add_argument("password")

	args = parser.parse_args()
	method = args.method_type
	password = str(args.password)

	if  method == "encode":
		encoded_pw = encoded(password)

		print(encoded_pw)

	elif method == "decode":
		decoded_pw = decoded(password)

		print(decoded_pw)
	else:
		print("Please enter a method: encode|decode")

arg_parse()