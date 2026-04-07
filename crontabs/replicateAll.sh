NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas ReplicateAll Script:"
echo $NOW
echo "Running downloadtables.py all."
cd {fullfilepath}/pythonReplicateData/
pwd
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/downloadtables.py all odl_usr
sleep 2
echo "Now running SQLLDR along with a MV Refresh:"
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/sqlldrData.py all 
echo "ReplicateAll completed."
DONE=$( date '+%F_%H:%M:%S' )
echo $DONE
