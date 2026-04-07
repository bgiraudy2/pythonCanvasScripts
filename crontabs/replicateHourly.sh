NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas ReplicateHourly Script:" 
echo $NOW
echo "Running downloadtables.py hourly." 
cd {fullfilepath}/pythonReplicateData/
pwd
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/downloadtables.py hourly odl_usr 
sleep 2
echo "Now running SQLLDR along with a MV Refresh:" 
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/sqlldrData.py hourly
echo "Now running Provisioning script:"
python3.6 {fullfilepath}/pythonProvisions/canvasProvisions.py
provstatus=$(head -n 1 {fullfilepath}/pythonProvisions/provisions_status)
echo $provstatus
if [[ $provstatus == "successful" ]]
then
	echo "Now running CanvasAPI:"
	python3.6 {fullfilepath}/pythonCanvasAPI/canvasApi.py 
	echo "Hourly refresh completed."
	DONE=$( date '+%F_%H:%M:%S' )
	echo $DONE
elif [[ $provstatus == "failed" ]]
then
	echo "Provisions failed for some reason. Exiting crontab now!"
	python3.6 {fullfilepath}/pythonProvisions/provisions_fail_email.py
	exit 1
fi
