import datetime

import prometheus_api_client
from DataProviders import *
import config

api = prometheus_api_client.PrometheusConnect(url=config.PROMETHEUS_URL, disable_ssl=True)
end = datetime.datetime.now()
start = end - datetime.timedelta(minutes=30)

# p = PodDataProvider(api, start_time=start, end_time=end, step=30)
# pod_data = p.get_data()

# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.cpu_usage)
#
# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.memory_usage)
#
# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.network_rx_bytes)
#
#
# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.network_tx_bytes)
#
# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.disk_read_bytes)
#
# for _,pod in pod_data.items():
#     print(pod.pod_name ,pod.namespace , pod.node_name)
#     print(pod.disk_write_bytes)
#
# for _,pod in pod_data.items():
#     print(pod.pod_name)
#     print(pod.cpu_request , pod.memory_request)
#


n = NodeDataProvider(api,start,end,10)
node_data= n.get_data()

# for _,node in node_data.items():
#     print(node.node_name)
#     print(node.cpu_usage)

# for _,node in node_data.items():
#     print(node.node_name)
#     print(node.memory_usage)

# for _,node in node_data.items():
#     print(node.node_name)
#     print(node.network_rx_bytes)

# for _, node in node_data.items():
#     print(node.node_name)
#     print(node.network_tx_bytes)


# for _, node in node_data.items():
#     print(node.node_name)
#     print(node.disk_read_bytes)
#
# for _, node in node_data.items():
#     print(node.node_name)
#     print(node.disk_write_bytes)

# for _, node in node_data.items():
#     print(node.node_name , node.cpu_limit , node.memory_limit , node.instance_type)