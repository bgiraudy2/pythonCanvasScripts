NOW=$( date '+%F_%H:%M:%S' )
echo "Canvas orgProcess Script:"
echo $NOW
echo "Running Historical Provisioning:"
cd {fullfilepath}/pythonProvisions/
pwd
python3.6 {fullfilepath}/pythonProvisions/canvasProvisions.py --historical true
echo "Now running Historical CanvasAPI:"
python3.6 {fullfilepath}/pythonCanvasAPI/canvasApi.py --historical true
echo "Historical Process completed."
DONE=$( date '+%F_%H:%M:%S' )
echo $DONE

