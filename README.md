# Website Metrics Pipeline

![](docs/arch-diag.png)

# Most Common Command

```
# Clone the repo
git clone https://github.com/rockoder/website-metrics-pipeline.git

# Create virtual env for running the demo
python -m venv demo

# Install the dependencies
pip install -r requirements.txt

# Run the producer. Remove '&' at the end if you don't want to run in background
python metricsproducer/metricsproducer.py --env test &

# Run the consumer. Remove '&' at the end if you don't want to run in background
python metricsconsumer/metricsconsumer.py --env test &

```

Logs will be generated in logs directory

