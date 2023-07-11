import datetime
from Flaggers import *
import prometheus_api_client
from DataProviders import *
import config

api = prometheus_api_client.PrometheusConnect(url=config.PROMETHEUS_URL,disable_ssl=True)
end = datetime.datetime.now()
start = end - datetime.timedelta(minutes=30)


p = PodDataProvider(api,start_time=start,end_time=end,step=10)
n = NodeDataProvider(api,start,end,10)

pod_data = p.get_data()
node_data = n.get_data()

