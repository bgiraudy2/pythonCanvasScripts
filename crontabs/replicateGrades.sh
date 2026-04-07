NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas ReplicateGrades Script:"
echo $NOW
echo "Running downloadtables.py grades."
cd {fullfilepath}/pythonReplicateData/
pwd
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/downloadtables.py grades odl_usr
sleep 2
echo "Now running SQLLDR along with a MV Refresh:"
sleep 2
python3.6 {fullfilepath}/pythonReplicateData/sqlldrData.py grades
echo "ReplicateGrades completed."
DONE=$( date '+%F_%H:%M:%S' )
echo $DONE
