# Website Metrics Pipeline


# Most Common Command

git clone https://github.com/rockoder/website-metrics-pipeline.git
python -m venv demo
pip install -r requirements.txt
python metricsproducer/metricsproducer.py --env test
python metricsconsumer/metricsconsumer.py --env test

Logs will be generated in logs directory

