spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 3 \
    5349-a2-Final.py \
    --output $1
