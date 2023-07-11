import pandas as pd
import datetime


def get_network_bandwidth_map(file_path):
    bandwidth_map = {}
    file = open(file_path, 'r')
    file.readline()
    while True:
        line = file.readline()
        if line:
            data = line.split(sep=',')
            bandwidth_map[data[0]] = float(data[1])
        else:
            break
    return bandwidth_map


def get_ebs_bandwidths(ec2_client, instance_type_list):
    res = ec2_client.describe_instance_types(InstanceTypes=instance_type_list)
    ebs_bandwidth_map = {}
    for e in res['InstanceTypes']:
        if 'EbsInfo' in e and 'EbsOptimizedInfo' in e['EbsInfo']:
            ebs_bandwidth_map[e['InstanceType']] = e['EbsInfo']['EbsOptimizedInfo']['BaselineBandwidthInMbps'] * 125000
    return ebs_bandwidth_map


def get_windows(data, timestamps, threshold, min_diff, max_win_size):
    all_windows = []
    i = 0
    while i < len(data):
        l = timestamps.iloc[i]
        if data.iloc[i] < threshold:
            i += 1
            continue

        while i < len(data) and data.iloc[i] > threshold:
            if timestamps.iloc[i] - l > max_win_size:
                break
            i += 1

        r = timestamps.iloc[i - 1]
        all_windows.append([l, r])
    if len(all_windows) == 0:
        return []
    compressed_windows = [all_windows[0]]
    for i in range(1, len(all_windows)):
        if (all_windows[i][0] - compressed_windows[-1][1]) / 1000 < min_diff:
            compressed_windows[-1][1] = all_windows[i][1]
        else:
            compressed_windows.append(all_windows[i])
    return compressed_windows


def get_instance_types_list(node_data):
    instance_type_set = set({})
    for _, node in node_data.items():
        if node.instance_type is None:
            print(f"instance type not known for node {node.nodename}")

        instance_type_set.add(node.instance_type)
    return list(instance_type_set)


# def get_simple_ratio(a, b):
#     g = math.gcd(a, b)
#     return f"{a // g}:{b // g}"


def group_pods_by_nodes(pod_data):
    node_pod_dict = {}
    for pod_name in pod_data.keys():
        if pod_data[pod_name].node_name not in node_pod_dict:
            node_pod_dict[pod_data[pod_name].node_name] = []
        node_pod_dict[pod_data[pod_name].node_name].append(pod_data[pod_name])
    return node_pod_dict


# def flag_pods_by_fragmentation(pod_data, threshold):
#     _table = []
#     for (namespace, pod_name), pod_item in pod_data.items():
#         try:
#             cpu_fragmentation = (pod_item.cpu_request - pod_item.cpu_usage['values']).apply(lambda t: max(0, t))
#             memory_fragmentation = (pod_item.memory_request - pod_item.memory_usage['values']).apply(lambda t: max(0, t))
#
#             cpu_afrr = cpu_fragmentation.mean() / pod_item.cpu_request
#             mem_afrr = memory_fragmentation.mean() / pod_item.memory_request
#
#             if cpu_afrr > threshold or mem_afrr > threshold:
#                 row = [namespace, pod_name, cpu_afrr, mem_afrr]
#                 if cpu_afrr > threshold:
#                     row[2] = f'[!]{row[2]}'
#                 if mem_afrr > threshold:
#                     row[3] = f'[!]{row[3]}'
#                 _table.append(row)
#
#         except Exception as e:
#             print(e)
#
#     df = pd.DataFrame(_table, columns=["namespace", "pod_name", "avg_fragmentation_to_request_ratio(cpu)",
#                                        "avg_fragmentation_to_request_ratio(memory)"])
#     return df


# class PodFragmentationFlagger:
#     def __init__(self, pod_data, threshold):
#         self.pod_data = pod_data
#         self.threshold = threshold
#
#     def flag(self):
#         _table = []
#         for (namespace, pod_name), pod_item in self.pod_data.items():
#             cpu_fragmentation = pod_item.cpu_request - pod_item.cpu_usage['values']
#             memory_fragmentation = pod_item.memory_request - pod_item.memory_usage['values']
#             cpu_fragmentation.apply(lambda t: max(0, t))
#             memory_fragmentation.apply(lambda t: max(0, t))
#             cpu_afrr = cpu_fragmentation.mean() / pod_item.cpu_request
#             mem_afrr = memory_fragmentation.mean() / pod_item.memory_request
#
#             if cpu_afrr > self.threshold or mem_afrr > self.threshold:
#                 row = [namespace, pod_name, cpu_afrr, mem_afrr]
#                 if cpu_afrr > self.threshold:
#                     row[2] = f'[!]{row[2]}'
#                 if mem_afrr > self.threshold:
#                     row[3] = f'[!]{row[3]}'
#                 _table.append(row)
#
#         df = pd.DataFrame(_table, columns=["namespace", "pod_name", "avg_fragmentation_to_request_ratio(cpu)",
#                                            "avg_fragmentation_to_request_ratio(memory)"])
#         return df


def flag_pods_for_wrong_cpu_requests(pod_data, margin=1, threshold=0.7):
    _table = []
    for (namespace, pod_name), pod in pod_data.items():
        try:
            avg_cpu = pod.cpu_usage['values'].mean()
            stddev_cpu = pod.cpu_usage['values'].std()
            exp_cpu_req = avg_cpu + margin * stddev_cpu

            bad_cpu_request = abs(pod.cpu_request - exp_cpu_req) / exp_cpu_req > threshold

            if bad_cpu_request:
                _table.append(
                    [namespace, pod_name,
                     pod.cpu_request, pod.cpu_limit, avg_cpu, exp_cpu_req, pod.cpu_usage['values'].quantile(0.95),
                     pod.cpu_usage['values'].quantile(0.99), pod.cpu_usage['values'].max()
                     ])
        except Exception as e:
            print(e, namespace, pod_name)

    df = pd.DataFrame(_table, columns=["Namespace", "Pod Name",
                                       "CPU Request", "CPU Limit", "Avg CPU Usage(Cores)", "Suggested Request",
                                       "90%tile CPU Usage",
                                       "90%tile CPU Usage", "Max CPU Usage"])
    return df


def flag_pods_for_wrong_memory_requests(pod_data, margin=1, threshold=0.7):
    _table = []
    for (namespace, pod_name), pod in pod_data.items():
        try:

            avg_mem = pod.memory_usage['values'].mean()
            stddev_mem = pod.memory_usage['values'].std()
            exp_memory_req = avg_mem + margin * stddev_mem

            bad_memory_request = abs(pod.memory_request - exp_memory_req) / exp_memory_req > threshold

            if bad_memory_request:
                _table.append(
                    [namespace, pod_name,
                     pod.memory_request, pod.memory_limit, avg_mem, exp_memory_req,
                     pod.memory_usage['values'].quantile(0.95),
                     pod.memory_usage['values'].quantile(0.99), pod.memory_usage['values'].max()
                     ])
        except Exception as e:
            print(e, namespace, pod_name)

    df = pd.DataFrame(_table, columns=["Namespace", "Pod Name",
                                       "Memory Request", "Memory Limit", "Avg Memory Usage", "Suggested Request",
                                       "90%tile Memory Usage",
                                       "90%tile Memory Usage", "Max Memory Usage"])
    return df


# class WrongPodRequestsFlagger:
#
#     def __init__(self, pod_data, margin=1, threshold=0.7):
#         self.margin = margin
#         self.pod_data = pod_data
#         self.threshold = threshold
#
#     def flag(self):
#         _table = []
#         for (namespace, pod_name), pod in self.pod_data.items():
#
#             avg_cpu = pod.cpu_usage['values'].mean()
#             stddev_cpu = pod.cpu_usage['values'].std()
#             exp_cpu_req = avg_cpu + self.margin * stddev_cpu
#
#             avg_mem = pod.memory_usage['values'].mean()
#             stddev_mem = pod.memory_usage['values'].std()
#             exp_memory_req = avg_mem + self.margin * stddev_mem
#
#             bad_cpu_request = abs(pod.cpu_request - exp_cpu_req) / exp_cpu_req > self.threshold
#             bad_memory_request = abs(pod.memory_request - exp_memory_req) / exp_memory_req > self.threshold
#
#             if bad_memory_request or bad_cpu_request:
#                 _table.append(
#                     [namespace, pod_name, pod.cpu_request, pod.memory_request, avg_cpu, avg_mem, exp_cpu_req,
#                      exp_memory_req])
#
#         df = pd.DataFrame(_table, columns=["namespace", "pod_name", "cpu_request", "memory_request", "avg_cpu_usage(m)",
#                                            "avg_mem_usage(bytes)", "exp_cpu_usage",
#                                            "exp_mem_usage"])
#         return df

def calculate_skewness(node_compute_to_memory_ratio, pod_compute_to_memory_ratio):
    return abs(node_compute_to_memory_ratio - pod_compute_to_memory_ratio) / node_compute_to_memory_ratio


def flag_pods_by_wrong_node_placement_by_requests(pod_data, node_data, threshold):
    node_pod_dict = group_pods_by_nodes(pod_data)
    _table = []
    for node_name in node_pod_dict.keys():
        try:
            node = node_data[node_name]
            node_compute_to_memory_ratio = node.cpu_limit / node.memory_limit
            for pod in node_pod_dict[node_name]:

                if pod.cpu_request == 0 or pod.memory_request == 0:
                    print(
                        f"Pod {pod.pod_name} with CPU request :{pod.cpu_request} , Memory request :  {pod.memory_request}")
                    continue

                pod_compute_to_memory_ratio = pod.cpu_request / pod.memory_request
                pod_skewness = calculate_skewness(node_compute_to_memory_ratio, pod_compute_to_memory_ratio)

                if pod_skewness > threshold:
                    _table.append(
                        [node_name, node.instance_type, pod.namespace, pod.pod_name, node_compute_to_memory_ratio,
                         pod_compute_to_memory_ratio])
        except Exception as e:
            print(e)
    df = pd.DataFrame(_table, columns=["node_name", "node instance type", "namespace", "pod_name",
                                       "node cpu/memory", "pod cpu/memory"])
    return df


# class WrongNodePlacementFlaggerBasedOnUsage:
#     def calculate_skewness(self, node_compute_to_memory_ratio, pod_compute_to_memory_ratio):
#         return abs(node_compute_to_memory_ratio - pod_compute_to_memory_ratio) / node_compute_to_memory_ratio
#
#     def __init__(self, pod_data, node_data, threshold, margin):
#         self.pod_data = pod_data
#         self.node_data = node_data
#         self.threshold = threshold
#         self.margin = margin
#
#     def flag(self):
#         node_pod_dict = group_pods_by_nodes(self.pod_data)
#         _table = []
#         for node_name in node_pod_dict.keys():
#             node = self.node_data[node_name]
#             node_compute_to_memory_ratio = node.cpu_limit / node.memory_limit
#             for pod in node_pod_dict[node_name]:
#                 avg_cpu = pod.cpu_usage['values'].mean()
#                 stddev_cpu = pod.cpu_usage['values'].std()
#                 exp_cpu_req = avg_cpu + self.margin * stddev_cpu
#
#                 avg_mem = pod.memory_usage['values'].mean()
#                 stddev_mem = pod.memory_usage['values'].std()
#                 exp_memory_req = avg_mem + self.margin * stddev_mem
#
#                 pod_compute_to_memory_ratio = exp_cpu_req / exp_memory_req
#
#                 pod_skewness = self.calculate_skewness(node_compute_to_memory_ratio, pod_compute_to_memory_ratio)
#
#                 if pod_skewness > self.threshold:
#                     _table.append([node_name, pod.namespace, pod.pod_name, exp_cpu_req, exp_memory_req,
#                                    node_compute_to_memory_ratio, pod_compute_to_memory_ratio])
#         df = pd.DataFrame(_table, columns=["node_name", "namespace", "pod_name", "pod_exp_cpu", "pod_exp_memory",
#                                            "node cpu/memory", "pod cpu/memory"])
#         return df


def flag_nodes_by_high_avg_cpu_utilization(node_data, threshold):
    bad_node_list = []
    for _, node in node_data.items():
        try:

            if node.cpu_usage is None:
                print("CPU usage data not available for node ", node.node_name)
                continue

            if node.cpu_usage['values'].mean() > node.cpu_limit * threshold:
                bad_node_list.append(node)
        except Exception as e:
            print(e)
    _table = []
    for node in bad_node_list:
        _table.append(
            [node.node_name, node.instance_type, node.cpu_limit, node.cpu_usage['values'].mean() / node.cpu_limit])
    df = pd.DataFrame(_table, columns=['node name', "instance type", "cpu limit", 'Avg CPU Utilization'])
    return df


# class NodeHighAvgCPUUtilizationFlagger:
#     def __init__(self, node_data, threshold):
#         self.node_data = node_data
#         self.threshold = threshold
#
#     def flag(self):
#         bad_node_list = []
#         for _, node in self.node_data.items():
#             if node.cpu_usage['values'].mean() > node.cpu_limit * self.threshold:
#                 bad_node_list.append(node)
#         _table = []
#         for node in bad_node_list:
#             _table.append([node.node_name, node.cpu_usage['values'].mean() / node.cpu_limit])
#         df = pd.DataFrame(_table, columns=['NodeName', 'Avg CPU Utilization'])
#         return df

def flag_nodes_by_high_probability_of_high_cpu_utilization(node_data, cpu_util_threshold, prob_limit):
    _table = []
    for _, node in node_data.items():

        try:

            if node.cpu_usage is None:
                print("CPU usage data not available for node ", node.node_name)
                continue

            high_usage_freq = 0
            for cpu_usage in node.cpu_usage['values']:
                high_usage_freq += cpu_usage > cpu_util_threshold * node.cpu_limit
            if high_usage_freq > prob_limit * len(node.cpu_usage):
                _table.append(
                    [node.node_name, node.instance_type, node.cpu_limit, high_usage_freq / len(node.cpu_usage)])
        except Exception as e:
            print(e)
    df = pd.DataFrame(_table, columns=['node name', "instance type", "cpu limit", 'High CPU Utilization Probability'])
    return df


#
# class NodeHighCPUUtilizationHighProbFlagger:
#     def __init__(self, node_data, cpu_util_threshold, prob_limit):
#         self.node_data = node_data
#         self.cpu_util_threshold = cpu_util_threshold
#         self.prob_limit = prob_limit
#
#     def flag(self):
#         _table = []
#         for _, node in self.node_data.items():
#             high_usage_freq = 0
#             for cpu_usage in node.cpu_usage['values']:
#                 high_usage_freq += cpu_usage > self.cpu_util_threshold * node.cpu_limit
#             if high_usage_freq > self.prob_limit * len(node.cpu_usage):
#                 _table.append([node.node_name, high_usage_freq / len(node.cpu_usage)])
#         df = pd.DataFrame(_table, columns=['node_name', 'High CPU Utilization Probability'])
#         return df


def flag_nodes_by_high_avg_memory_utilization(node_data, threshold):
    bad_node_list = []
    for _, node in node_data.items():
        try:

            if node.memory_usage is None:
                print("Memory usage data not available for node ", node.node_name)
                continue

            if node.memory_usage['values'].mean() > threshold * node.memory_limit:
                bad_node_list.append([node.node_name, node.instance_type, node.memory_limit,
                                      node.memory_usage['values'].mean() / node.memory_limit])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'memory limit', 'Avg Memory Utilization'])
    return df


# class NodeHighAvgMemoryUtilizationFlagger:
#     def __init__(self, node_data, threshold):
#         self.node_data = node_data
#         self.threshold = threshold
#
#     def flag(self):
#         bad_node_list = []
#         for _, node in self.node_data.items():
#             if node.memory_usage['values'].mean() > self.threshold * node.memory_limit:
#                 bad_node_list.append([node.node_name, node.memory_usage['values'].mean() / node.memory_limit])
#         df = pd.DataFrame(bad_node_list, columns=['node_name', 'Avg Memory Utilization'])
#         return df

def flag_nodes_by_high_probability_of_high_memory_utilization(node_data, memory_util_threshold, prob_limit):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            high_usage_freq = 0

            if node.memory_usage is None:
                print("Memory usage data not available for node ", node.node_name)
                continue

            for mem_use in node.memory_usage['values']:
                high_usage_freq += mem_use > memory_util_threshold * node.memory_limit
            if high_usage_freq > prob_limit * len(node.memory_usage):
                bad_node_list.append(
                    [node.node_name, node.instance_type, node.memory_limit, high_usage_freq / len(node.memory_usage)])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list,
                      columns=['node name', 'instance type', 'memory limit', 'High Memory Utilization Probability'])
    return df


# class NodeHighMemoryUtilizationHighProbFlagger:
#     def __init__(self, node_data, memory_util_threshold, prob_limit):
#         self.node_data = node_data
#         self.memory_util_threshold = memory_util_threshold
#         self.prob_limit = prob_limit
#
#     def flag(self):
#         bad_node_list = []
#         for _, node in self.node_data.items():
#             high_usage_freq = 0
#             for mem_use in node.memory_usage['values']:
#                 high_usage_freq += mem_use > self.memory_util_threshold * node.memory_limit
#             if high_usage_freq > self.prob_limit * len(node.memory_usage):
#                 bad_node_list.append([node.node_name, high_usage_freq / len(node.memory_usage)])
#         df = pd.DataFrame(bad_node_list, columns=['node_name', 'High Memory Utilization Probability'])
#         return df


def flag_nodes_by_high_avg_network_tx_bytes(node_data, threshold, bandwidth_map):
    bad_node_list = []

    for _, node in node_data.items():
        try:
            if node.instance_type not in bandwidth_map:
                print(f'bandwidth not available for instance_type : {node.instance_type}')
                continue
            if node.network_tx_bytes is None:
                print("Network rx bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type] * 1.25 * 1E8  # conversion from Gbps to bytes/sec

            if node.network_tx_bytes['values'].mean() > threshold * node_bandwidth_limit:
                bad_node_list.append(
                    [node.node_name, node.instance_type, node_bandwidth_limit, node.network_tx_bytes['values'].mean()])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'node bandwidth baselimit(bytes/sec)',
                                              'Avg Network Transmit rate (bytes/sec)',
                                              ])
    return df


def flag_nodes_by_high_probability_of_high_network_tx_bytes(node_data, threshold, prob_limit, bandwidth_map):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            if node.instance_type not in bandwidth_map:
                print(f'bandwidth not available for instance_type : {node.instance_type}')
                continue
            if node.network_tx_bytes is None:
                print("Network rx bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type] * 1.25 * 1E8  # conversion from Gbps to bytes/sec
            high_usage_freq = 0
            for tx_bytes in node.network_tx_bytes['values']:
                high_usage_freq += tx_bytes > threshold * node_bandwidth_limit
            if high_usage_freq > prob_limit * len(node.network_tx_bytes):
                bad_node_list.append([node.node_name, node.instance_type, node_bandwidth_limit,
                                      high_usage_freq / len(node.network_tx_bytes)])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'node baseline bandwidth',
                                              'High Network Transmit Bytes Probability'])
    return df


def flag_nodes_by_high_avg_network_rx_bytes(node_data, threshold, bandwidth_map):
    bad_node_list = []

    for _, node in node_data.items():
        try:

            if node.instance_type not in bandwidth_map:
                print(f'bandwidth not available for instance_type : {node.instance_type}')
                continue
            if node.network_rx_bytes is None:
                print("Network rx bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type] * 1.25 * 1E8  # conversion from Gbps to bytes/sec

            if node.network_rx_bytes['values'].mean() > threshold * node_bandwidth_limit:
                bad_node_list.append(
                    [node.node_name, node.instance_type, node_bandwidth_limit, node.network_rx_bytes['values'].mean()])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', "instance type", 'node bandwidth baselimit(bytes/sec)',
                                              'Avg Network Received rate (bytes/sec)'
                                              ])
    return df


def flag_nodes_by_high_probability_of_high_network_rx_bytes(node_data, threshold, prob_limit, bandwidth_map):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            if node.instance_type not in bandwidth_map:
                print(f'bandwidth not available for instance_type : {node.instance_type}')
                continue

            if node.network_rx_bytes is None:
                print("Network rx bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type] * 1.25 * 1E8  # conversion from Gbps to bytes/sec
            high_usage_freq = 0
            for rx_bytes in node.network_rx_bytes['values']:
                high_usage_freq += rx_bytes > threshold * node_bandwidth_limit
            if high_usage_freq > prob_limit * len(node.network_rx_bytes):
                bad_node_list.append([node.node_name, node.instance_type, node_bandwidth_limit,
                                      high_usage_freq / len(node.network_rx_bytes)])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'node baseline bandwidth',
                                              'High Network Received Bytes Probability'])
    return df


# TODO : update the below two functions
def flag_nodes_by_high_avg_disk_total_bytes(node_data, threshold, bandwidth_map):
    bad_node_list = []

    for _, node in node_data.items():
        try:
            if node.instance_type not in bandwidth_map:
                print(f'ebs  bandwidth not available for instance_type : {node.instance_type}')
                continue
            if node.disk_total_bytes is None:
                print("Total disk bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type]

            if node.disk_total_bytes['values'].mean() > threshold * node_bandwidth_limit:
                bad_node_list.append(
                    [node.node_name, node.instance_type, node_bandwidth_limit, node.disk_total_bytes['values'].mean()])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'node bandwidth baselimit(bytes/sec)',
                                              'Avg Network Transmit rate (bytes/sec)',
                                              ])
    return df


def flag_nodes_by_high_probability_of_high_disk_total_bytes(node_data, threshold, prob_limit, bandwidth_map):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            if node.instance_type not in bandwidth_map:
                print(f'ebs bandwidth not available for instance_type : {node.instance_type}')
                continue
            if node.disk_total_bytes is None:
                print("Total disk bytes data not available for node ", node.node_name)
                continue

            node_bandwidth_limit = bandwidth_map[node.instance_type]
            high_usage_freq = 0
            for disk_bytes in node.disk_total_bytes['values']:
                high_usage_freq += disk_bytes > threshold * node_bandwidth_limit
            if high_usage_freq > prob_limit * len(node.disk_total_bytes):
                bad_node_list.append([node.node_name, node.instance_type, node_bandwidth_limit,
                                      high_usage_freq / len(node.memory_usage)])
        except Exception as e:
            print(e)
    df = pd.DataFrame(bad_node_list, columns=['node name', 'instance type', 'node baseline bandwidth',
                                              'High disk read/write Bytes Probability'])
    return df


def search_lb(df, val):
    tl = -1
    tr = len(df)

    while tr - tl > 1:
        tm = (tl + tr) // 2
        if df.iloc[tm] > val:
            tr = tm
        else:
            tl = tm
    return tl


def search_rb(df, val):
    tl = -1
    tr = len(df)

    while tr - tl > 1:
        tm = (tl + tr) // 2
        if df.iloc[tm] > val:
            tr = tm
        else:
            tl = tm
    return tr


def mark_culprit_pods_for_high_cpu(node_item, pod_list, min_window_diff, max_win_size, threshold_fraction):
    threshold = threshold_fraction * node_item.cpu_limit
    bad_windows = get_windows(node_item.cpu_usage['values'], node_item.cpu_usage['timestamp'], threshold,
                              min_window_diff, max_win_size)

    _table = []
    for win in bad_windows:
        _table.append(
            ['Window :', str(datetime.datetime.fromtimestamp(win[0])), str(datetime.datetime.fromtimestamp(win[1]))])
        # print('window: ', [str(datetime.datetime.fromtimestamp(x)) for x in win])
        l, r = win

        _pods = []
        for pod in pod_list:

            if pod.cpu_usage is None:
                print("CPU usage not available for pod ", pod.pod_name)
                continue

            if pod.cpu_usage['timestamp'].iloc[0] > r or pod.cpu_usage['timestamp'].iloc[-1] < l:
                continue
            l_idx = search_lb(pod.cpu_usage['timestamp'], l)
            r_idx = search_rb(pod.cpu_usage['timestamp'], r)
            win_df = pod.cpu_usage['values'].iloc[l_idx:r_idx + 1]
            _pods.append([pod.pod_name, win_df.mean(), win_df.max(), pod.cpu_request])
        if len(_pods) == 0:
            return
        max_avg_pod = max(_pods, key=lambda x: x[1])
        max_max_pod = max(_pods, key=lambda x: x[2])
        max_cpu_above_request_pod = max(_pods, key=lambda x: (x[2] - x[3]))
        _table.append(['__', 'pod_name', 'cpu', 'request'])
        _table.append(['max avg cpu', max_avg_pod[0], max_avg_pod[1], max_avg_pod[3]])
        _table.append(['max cpu usage', max_max_pod[0], max_max_pod[2], max_max_pod[3]])
        if max_cpu_above_request_pod[2] > max_cpu_above_request_pod[3]:
            _table.append(['max cpu above request', max_cpu_above_request_pod[0], max_cpu_above_request_pod[1],
                           max_cpu_above_request_pod[3]])
    df = pd.DataFrame(_table)
    return df


def mark_culprit_pods_for_high_memory(node_item, pod_list, min_window_diff, max_win_size, threshold_fraction):
    threshold = node_item.memory_limit * threshold_fraction
    bad_windows = get_windows(node_item.memory_usage['values'], node_item.memory_usage['timestamp'], threshold,
                              min_window_diff, max_win_size)
    _table = []
    for win in bad_windows:
        _table.append(
            ['Window :', str(datetime.datetime.fromtimestamp(win[0])), str(datetime.datetime.fromtimestamp(win[1]))])

        l, r = win

        _pods = []
        for pod in pod_list:
            if pod.memory_usage is None:
                continue
            if pod.memory_usage['timestamp'].iloc[0] > r or pod.memory_usage['timestamp'].iloc[-1] < l:
                continue
            l_idx = search_lb(pod.memory_usage['timestamp'], l)
            r_idx = search_rb(pod.memory_usage['timestamp'], r)
            win_df = pod.memory_usage['values'].iloc[l_idx:r_idx + 1]
            _pods.append([pod.pod_name, win_df.mean(), win_df.max(), pod.memory_request])

        if len(_pods) == 0:
            return

        max_avg_pod = max(_pods, key=lambda x: x[1])
        max_max_pod = max(_pods, key=lambda x: x[2])
        max_memory_above_request_pod = max(_pods, key=lambda x: (x[2] - x[3]))

        _table.append(['__', 'pod_name', 'memory', 'request'])
        _table.append(['max avg memory', max_avg_pod[0], max_avg_pod[1], max_avg_pod[3]])
        _table.append(['max memory usage', max_max_pod[0], max_max_pod[2], max_max_pod[3]])
        if max_memory_above_request_pod[2] > max_memory_above_request_pod[3]:
            _table.append(['max cpu above request', max_memory_above_request_pod[0], max_memory_above_request_pod[1],
                           max_memory_above_request_pod[3]])
    df = pd.DataFrame(_table)
    return df


def mark_culprit_pods_for_high_tx_bytes(node_item, pod_list, min_window_diff, max_win_size, threshold_fraction,
                                        bandwidth_map):
    node_baseline_limit = bandwidth_map[node_item.instance_type]
    threshold = node_baseline_limit * threshold_fraction
    bad_windows = get_windows(node_item.network_tx_bytes['values'], node_item.network_tx_bytes['timestamp'], threshold,
                              min_window_diff, max_win_size)
    _table = []
    for win in bad_windows:
        _table.append(
            ['Window :', str(datetime.datetime.fromtimestamp(win[0])), str(datetime.datetime.fromtimestamp(win[1]))])

        l, r = win

        _pods = []
        for pod in pod_list:
            if pod.network_tx_bytes is None:
                continue
            if pod.network_tx_bytes['timestamp'].iloc[0] > r or pod.network_tx_bytes['timestamp'].iloc[-1] < l:
                continue
            l_idx = search_lb(pod.network_tx_bytes['timestamp'], l)
            r_idx = search_rb(pod.network_tx_bytes['timestamp'], r)
            win_df = pod.network_tx_bytes['values'].iloc[l_idx:r_idx + 1]
            _pods.append([pod.pod_name, win_df.mean(), win_df.max()])

        if len(_pods) == 0:
            return

        max_avg_pod = max(_pods, key=lambda x: x[1])
        max_max_pod = max(_pods, key=lambda x: x[2])

        _table.append(['__', 'pod_name', 'tx bytes'])
        _table.append(['max avg tx bytes', max_avg_pod[0], max_avg_pod[1], max_avg_pod[2]])
        _table.append(['max tx bytes', max_max_pod[0], max_max_pod[2], max_max_pod[2]])
    df = pd.DataFrame(_table)
    return df


def mark_culprit_pods_for_high_rx_bytes(node_item, pod_list, min_window_diff, max_win_size, threshold_fraction,
                                        bandwidth_map):
    node_baseline_limit = bandwidth_map[node_item.instance_type]
    threshold = node_baseline_limit * threshold_fraction
    bad_windows = get_windows(node_item.network_rx_bytes['values'], node_item.network_rx_bytes['timestamp'], threshold,
                              min_window_diff, max_win_size)
    _table = []
    for win in bad_windows:
        _table.append(
            ['Window :', str(datetime.datetime.fromtimestamp(win[0])), str(datetime.datetime.fromtimestamp(win[1]))])

        l, r = win

        _pods = []
        for pod in pod_list:
            if pod.network_rx_bytes is None:
                continue
            if pod.network_rx_bytes['timestamp'].iloc[0] > r or pod.network_rx_bytes['timestamp'].iloc[-1] < l:
                continue
            l_idx = search_lb(pod.network_rx_bytes['timestamp'], l)
            r_idx = search_rb(pod.network_rx_bytes['timestamp'], r)
            win_df = pod.network_rx_bytes['values'].iloc[l_idx:r_idx + 1]
            _pods.append([pod.pod_name, win_df.mean(), win_df.max()])

        if len(_pods) == 0:
            return

        max_avg_pod = max(_pods, key=lambda x: x[1])
        max_max_pod = max(_pods, key=lambda x: x[2])

        _table.append(['__', 'pod_name', 'rx bytes'])
        _table.append(['max avg rx bytes', max_avg_pod[0], max_avg_pod[1], max_avg_pod[2]])
        _table.append(['max rx bytes', max_max_pod[0], max_max_pod[2], max_max_pod[2]])
    df = pd.DataFrame(_table)
    return df


def mark_culprit_pods_for_total_disk_bytes(node_item, pod_list, min_window_diff, max_win_size, threshold_fraction,
                                           bandwidth_map):
    node_baseline_limit = bandwidth_map[node_item.instance_type]
    threshold = node_baseline_limit * threshold_fraction
    bad_windows = get_windows(node_item.disk_total_bytes['values'], node_item.disk_total_bytes['timestamp'], threshold,
                              min_window_diff, max_win_size)
    _table = []
    for win in bad_windows:
        _table.append(
            ['Window :', str(datetime.datetime.fromtimestamp(win[0])), str(datetime.datetime.fromtimestamp(win[1]))])

        l, r = win

        _pods = []
        for pod in pod_list:
            if pod.disk_total_bytes is None:
                continue
            if pod.disk_total_bytes['timestamp'].iloc[0] > r or pod.disk_total_bytes['timestamp'].iloc[-1] < l:
                continue
            l_idx = search_lb(pod.disk_total_bytes['timestamp'], l)
            r_idx = search_rb(pod.disk_total_bytes['timestamp'], r)
            win_df = pod.disk_total_bytes['values'].iloc[l_idx:r_idx + 1]
            _pods.append([pod.pod_name, win_df.mean(), win_df.max()])

        if len(_pods) == 0:
            return

        max_avg_pod = max(_pods, key=lambda x: x[1])
        max_max_pod = max(_pods, key=lambda x: x[2])

        _table.append(['__', 'pod_name', 'disk total bytes'])
        _table.append(['max avg rx bytes', max_avg_pod[0], max_avg_pod[1], max_avg_pod[2]])
        _table.append(['max rx bytes', max_max_pod[0], max_max_pod[2], max_max_pod[2]])
    df = pd.DataFrame(_table)
    return df

# def get_pod_network_rx_bytes_report(pod_data, bandwidth_map):
#     _table = []
#     for _, pod in pod_data.items():
#
#         if pod.network_rx_bytes is not None:
#             print(pod.node_name)
#             _table.append([pod.namespace, pod.pod_name, pod.network_rx_bytes['values'].mean(),
#                            pod.network_rx_bytes['values'].quantile(0.9), pod.network_rx_bytes['values'].max()])
#     df = pd.DataFrame(_table, columns=['namespace', 'pod_name', 'avg rx bytes', '90th %tile', "max_rx_bytes"])
#     print(df)
#     return df
#
#
# def get_pod_network_tx_bytes_report(pod_data, bandwidth_map):
#     _table = []
#     for _, pod in pod_data.items():
#
#         if pod.network_tx_bytes is not None:
#             print(pod.node_name)
#             _table.append([pod.namespace, pod.pod_name, pod.network_tx_bytes['values'].mean(),
#                            pod.network_tx_bytes['values'].quantile(0.9), pod.network_tx_bytes['values'].max()])
#     df = pd.DataFrame(_table, columns=['namespace', 'pod_name', 'avg tx bytes', '90th %tile', "max_tx_bytes"])
#     print(df)
#     return df

# def get_pod_disk_read_bytes_report(pod_data):
#     _table = []
#     for _, pod in pod_data.items():
#
#         if pod.disk_read_bytes is not None:
#             print(pod.node_name)
#             _table.append([pod.namespace, pod.pod_name, pod.disk_read_bytes['values'].mean(),
#                            pod.disk_read_bytes['values'].quantile(0.9), pod.disk_read_bytes['values'].max()])
#     df = pd.DataFrame(_table,
#                       columns=['namespace', 'pod_name', 'avg disk read bytes', '90th %tile', "max disk read bytes"])
#     print(df)
#     return df
#
#
# def get_pod_disk_write_bytes_report(pod_data):
#     _table = []
#     for _, pod in pod_data.items():
#
#         if pod.disk_write_bytes is not None:
#             _table.append(
#                 [pod.namespace, pod.pod_name, pod.disk_write_bytes['values'].mean(),
#                  pod.disk_write_bytes['values'].quantile(0.9),
#                  pod.disk_write_bytes['values'].max()])
#     df = pd.DataFrame(_table,
#                       columns=['namespace', 'pod_name', 'avg disk read bytes', '90th %tile', "max disk read bytes"])
#     print("fnsfsnnv n nf nfin ")
#     return df
