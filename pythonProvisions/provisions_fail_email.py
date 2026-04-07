import datetime
import os
import sys

subject = "Canvas Provisions Failure"
recipients = (
	'example@example.com', 
	'example2@example.com', 
	'example3@example.com'
	)	
message = f"""The pythonProvisions script reported a status of failure on {datetime.datetime.today()}. 
Please check the logfiles located at '/logs' for more information."""
for recipient in recipients:
	os.system(f"echo '{message}' | mailx -s '{subject}' {recipient}")
print("Email sent to the Data Systems Team")
