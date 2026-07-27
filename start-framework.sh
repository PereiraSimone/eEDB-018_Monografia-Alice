spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --py-files /data_quality_framework_pyspark-1.0.0-py3-none-any.whl \
    arquivo/app_analyses.py \
    --config-path s3://bucket-config/configs \
    --datasets pedidos,clientes