import pandas as pd
import re



class PodData:
    def __init__(self, pod_name=None, namespace=None, node_name=None, memory_request=0, cpu_request=0,cpu_limit = float('inf'),memory_limit = float("inf")):
        self.pod_name = pod_name
        self.namespace = namespace
        self.node_name = node_name

        self.cpu_request = cpu_request
        self.memory_request = memory_request
        self.cpu_usage = None
        self.memory_usage = None
        self.network_rx_bytes = None
        self.network_tx_bytes = None
        # self.disk_read_bytes = None
        # self.disk_write_bytes = None
        self.disk_total_bytes = None
        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit


class NodeData:
    def __init__(self, node_name):
        self.node_name = node_name
        self.instance_type = None
        self.cpu_limit = None
        self.memory_limit = None
        self.cpu_usage = None
        self.memory_usage = None
        self.cpu_utilization = None
        self.memory_utilization = None
        self.network_rx_bytes = None
        self.network_tx_bytes = None
        # self.disk_read_bytes = None
        # self.disk_write_bytes = None
        self.disk_total_bytes = None


class DataProvider:
    def __init__(self, prometheus_api):
        self.prometheus_api = prometheus_api


class PodDataProvider(DataProvider):

    def __init__(self, prometheus_api, start_time, end_time, step):
        super().__init__(prometheus_api)
        self.start_time = start_time
        self.end_time = end_time
        self.step = step

    def get_data(self):
        pod_data = {}
        # cpu usage of pod
        try:
            print("Getting pod cpu usage")
            pod_cpu_usage_res = self.prometheus_api.custom_query_range(
                query="sum(rate(container_cpu_usage_seconds_total{namespace!='',pod!='',instance!=''}[5m])) by(namespace,pod,instance)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod cpu usage")

            for _data in pod_cpu_usage_res:
                if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
                    print('pod or namespace not present in metric field')
                    continue

                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.cpu_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.cpu_usage = pod.cpu_usage.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error in getting cpu usage of pods")
            print(e)

        # pod memory usage
        try:
            pod_memory_usage_res = self.prometheus_api.custom_query_range(
                query="sum(container_memory_usage_bytes{namespace!='',pod!='',instance!=''}) by (pod,namespace,instance)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            for _data in pod_memory_usage_res:
                if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
                    print('pod or namespace not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.memory_usage = pod.memory_usage.astype({'timestamp': int, 'values': int})
        except Exception as e:
            print("Error while getting pod memory usage")
            print(e)
        #
        # pod network_rx_bytes
        try:
            print("Getting pod rx bytes")
            pod_rx_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(container_network_receive_bytes_total{namespace!='',pod!='',instance!=''}[5m]))by (namespace,pod,instance)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod rx bytes")
            for _data in pod_rx_bytes_res:
                if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
                    print('pod or namespace not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.network_rx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.network_rx_bytes = pod.network_rx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting pod network rx bytes")
            print(e)

        # pod network_tx_bytes
        try:
            print("Getting pod tx bytes")
            pod_tx_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(container_network_transmit_bytes_total{namespace!='',pod!='',instance!=''}[5m]))by (namespace,pod,instance)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Getting pod tx bytes")
            for _data in pod_tx_bytes_res:
                if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
                    print('pod or namespace not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.network_tx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.network_tx_bytes = pod.network_tx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting pod network tx bytes")
            print(e)
        # pod disk total bytes
        try:
            print("Getting disk total bytes for pods")
            pod_disk_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(container_fs_reads_bytes_total{namespace!='',pod!='',instance!=''}[5m])+rate(container_fs_writes_bytes_total{namespace!='',pod!='',instance!=''}[5m]))by (pod,namespace,instance)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received disk total bytes for pods")
            for _data in pod_disk_bytes_res:
                if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
                    print('pod or namespace not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.disk_total_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.disk_total_bytes = pod.disk_total_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting pods disk read+write data")
            print(e)

        # # pod disk write bytes
        # try:
        #     print("Getting disk write bytes for pods")
        #     pod_disk_read_bytes_res = self.prometheus_api.custom_query_range(
        #         query="sum(rate(container_fs_writes_bytes_total{namespace!='',pod!='',instance!=''}[5m]))by (pod,namespace,instance)",
        #         start_time=self.start_time,
        #         end_time=self.end_time,
        #         step=self.step
        #     )
        #     print("Received disk write bytes for pods")
        #     for _data in pod_disk_read_bytes_res:
        #         if 'pod' not in _data['metric'] or 'namespace' not in _data['metric']:
        #             print('pod or namespace not present in metric field')
        #             continue
        #         namespace = _data['metric']['namespace']
        #         pod_name = _data['metric']['pod']
        #         node_name = re.sub(".ec2.internal","",_data['metric']['instance'])
        #         if (namespace, pod_name) not in pod_data:
        #             pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
        #         pod = pod_data[(namespace, pod_name)]
        #         pod.disk_write_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
        #         pod.disk_write_bytes = pod.disk_write_bytes.astype({'timestamp': int, 'values': float})
        # except Exception as e:
        #     print("Error while getting pods disk read data")
        #     print(e)

        # cpu request
        try:
            print("Getting pod cpu requests")
            pod_cpu_req_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_requests{resource='cpu',namespace!='',pod!='',node!=''})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod cpu requests")
            for _data in pod_cpu_req_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = _data['metric']['pod']
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)

                pod = pod_data[(namespace, pod_name)]
                pod.cpu_request = float(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod cpu requests")
            print(e)

        # pod memory requests
        try:
            print("Getting pod memory requests")
            pod_memory_req_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_requests{resource='memory',namespace!='',pod!='',node!=''})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod memory requests")
            for _data in pod_memory_req_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = int(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod memory requests")
            print(e)

        try:
            print("Getting pod memory requests")
            pod_memory_req_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_requests{resource='memory',namespace!='',pod!='',node!=''})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod memory requests")
            for _data in pod_memory_req_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = int(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod memory requests")
            print(e)

        # cpu request
        try:
            print("Getting pod cpu requests")
            pod_cpu_req_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_requests{resource='cpu',namespace!='',pod!='',node!=''})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod cpu requests")
            for _data in pod_cpu_req_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = _data['metric']['pod']
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)

                pod = pod_data[(namespace, pod_name)]
                pod.cpu_request = float(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod cpu requests")
            print(e)

        # pod memory requests
        try:
            print("Getting pod memory requests")
            pod_memory_req_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_requests{resource='memory',namespace!='',pod!='',node!=''})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod memory requests")
            for _data in pod_memory_req_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = int(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod memory requests")
            print(e)

        #pod cpu limit

        try:
            print("Getting pod cpu limits")
            pod_cpu_limits_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_limits{resource='cpu'})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod cpu limits")
            for _data in pod_cpu_limits_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = float(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod memory requests")
            print(e)

        # pod cpu limit

        try:
            print("Getting pod memory limits")
            pod_cpu_limits_res = self.prometheus_api.custom_query_range(
                query="sum(kube_pod_container_resource_limits{resource='memory'})by(pod,namespace,node)",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received pod memory limits")
            for _data in pod_cpu_limits_res:
                if 'namespace' not in _data['metric'] or 'pod' not in _data['metric'] or 'node' not in _data['metric']:
                    print('pod or namespace or nodename not present in metric field')
                    continue
                namespace = _data['metric']['namespace']
                pod_name = _data['metric']['pod']
                node_name = re.sub(".ec2.internal", "", _data['metric']['node'])
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = int(_data['values'][0][1])
        except Exception as e:
            print("Error getting pod memory requests")
            print(e)
        return pod_data


class NodeDataProvider(DataProvider):
    def __init__(self, prometheus_api, start_time, end_time, step):
        super().__init__(prometheus_api)
        self.start_time = start_time
        self.end_time = end_time
        self.step = step

    def get_data(self):
        node_data = {}

        # cpu usage of node
        try:
            print("Getting node cpu usage")
            node_cpu_usage_res = self.prometheus_api.custom_query_range(
                query="sum(rate(node_cpu_seconds_total{mode!='idle',mode!='iowait',mode!='steal'}[2m]))by(instance)*on(instance)group_left(nodename) node_uname_info",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node cpu usage data")
            for _data in node_cpu_usage_res:

                if 'nodename' not in _data['metric']:
                    print("nodename field not present in metric")
                    continue

                node_name = _data['metric']['nodename']
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.cpu_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.cpu_usage = node.cpu_usage.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting node cpu usage data")
            print(e)

        try:
            print("Getting node memory usage")
            node_memory_usage_res = self.prometheus_api.custom_query_range(
                query="(node_memory_MemTotal_bytes - node_memory_MemFree_bytes- node_memory_Buffers_bytes - node_memory_Cached_bytes)*on(instance)group_left(nodename) node_uname_info",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node memory usage data")
            for _data in node_memory_usage_res:

                if 'nodename' not in _data['metric']:
                    print("nodename field not present in metric")
                    continue

                node_name = _data['metric']['nodename']
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.memory_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.memory_usage = node.memory_usage.astype({'timestamp': int, 'values': int})
        except Exception as e:
            print("Error while getting node memory usage data")
            print(e)

        try:
            print("Getting node network rx bytes ")
            node_rx_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(node_network_receive_bytes_total[2m])) by (instance)*on(instance)group_left(nodename) node_uname_info",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node network rx bytes")
            for _data in node_rx_bytes_res:

                if 'nodename' not in _data['metric']:
                    print("nodename field not present in metric")
                    continue

                node_name = _data['metric']['nodename']
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.network_rx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.network_rx_bytes = node.network_rx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting node network rx bytes")
            print(e)

        try:
            print("Getting node network tx bytes ")
            node_tx_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(node_network_transmit_bytes_total[2m])) by (instance)*on(instance)group_left(nodename) node_uname_info",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node network tx bytes")
            for _data in node_tx_bytes_res:

                if 'nodename' not in _data['metric']:
                    print("nodename field not present in metric")
                    continue

                node_name = _data['metric']['nodename']
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.network_tx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.network_tx_bytes = node.network_tx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting node network tx bytes")
            print(e)
        #node disk total read and writes

        try:
            print("Getting node disk total bytes ")
            node_disk_total_bytes_res = self.prometheus_api.custom_query_range(
                query="sum(rate(node_disk_written_bytes_total{device=~'nvme...'}[2m]) + rate(node_disk_read_bytes_total{device=~'nvme...'}[2m]))by(instance)*on(instance)group_left(nodename) node_uname_info",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node disk total bytes")
            for _data in node_disk_total_bytes_res:

                if 'nodename' not in _data['metric']:
                    print("nodename field not present in metric")
                    continue

                node_name = re.sub(".ec2.internal", "", _data['metric']['nodename'])
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)

                node_name = _data['metric']['nodename']
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.disk_total_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.disk_total_bytes = node.disk_total_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            print("Error while getting node disk write bytes")
            print(e)

        # cpu and memory capacity
        try:
            print("Getting node cpu capacity")
            node_cpu_cap_res = self.prometheus_api.custom_query_range(
                query="kube_node_status_capacity{resource='cpu'}",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node cpu capacity")
            for _data in node_cpu_cap_res:
                if 'node' not in _data['metric']:
                    print('node field not in metric')
                    continue

                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.cpu_limit = float(_data['values'][0][1])
        except Exception as e:
            print("Error while getting node cpu capacity")
            print(e)

        try:
            print("Getting node memory capacity")
            node_memory_cap_res = self.prometheus_api.custom_query_range(
                query="kube_node_status_capacity{resource='memory'}",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node memory capacity")
            for _data in node_memory_cap_res:
                if 'node' not in _data['metric']:
                    print('node field not in metric')
                    continue
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.memory_limit = float(_data['values'][0][1])
        except Exception as e:
            print("Error while getting node memory capacity")
            print(e)

        try:
            print("Getting node instance type")
            node_instance_type_res = self.prometheus_api.custom_query_range(
                query="kube_node_labels",
                start_time=self.start_time,
                end_time=self.end_time,
                step=self.step
            )
            print("Received node instance types")
            for _data in node_instance_type_res:
                if 'node' not in _data['metric']:
                    print('node field not in metric')
                    continue
                node_name = re.sub(".ec2.internal","", _data['metric']['node'])
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.instance_type = _data['metric']['label_node_kubernetes_io_instance_type']
        except Exception as e:
            print("Error while getting node instance type")
            print(e)
        return node_data

