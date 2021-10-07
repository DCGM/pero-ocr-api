source ~/env/tf/bin/activate
export PYTHONPATH=/home/ihradis/projects/2018-01-15_PERO/pero-live:/home/ihradis/projects/2018-01-15_PERO/pero-live/src:/home/ihradis/projects/2018-01-15_PERO/pero-ocr-live:/home/ihradis/projects/2018-01-15_PERO/pero_ocr_web

LOG_DATE=$(date '+%Y%m%d%H%M%S')
export TF_CUDNN_USE_AUTOTUNE=0

while true
do
    python3  -u ./processing_client/run_client.py  -c ./processing_client/config.ini --time-limit 0.5 --exit-on-done 2>&1 | tee -a api.$LOG_DATE.txt
    sleep 10
done
