NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas orgProcess Script:"
echo $NOW
echo "Running Org Provisioning:"
cd {fullfilepath}/pythonProvisions/
pwd
python3.6 {fullfilepath}/pythonProvisions/canvasProvisions.py --processorgs true
echo "Now running org CanvasAPI:"
python3.6 {fullfilepath}/pythonCanvasAPI/canvasApi.py --processorgs true
echo "OrgProcess completed."
DONE=$( date '+%F_%H:%M:%S' )
echo $DONE

