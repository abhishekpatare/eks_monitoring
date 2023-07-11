import datetime
import prometheus_api_client
from DataProviders import *
import config
from Flaggers import *
import os
import boto3

output_path_prefix = input("Output path: ") + '/reports'
if not os.path.exists(output_path_prefix):
    os.mkdir(output_path_prefix)


def write_report(msg, report, output_path):
    file = open(output_path, 'w')
    file.write(msg + "\n\n")
    file.close()
    report.to_csv(output_path, mode='a', index=False)


api = prometheus_api_client.PrometheusConnect(url=config.PROMETHEUS_URL, disable_ssl=True)
end = datetime.datetime.now()
start = end - datetime.timedelta(minutes=config.TIMEDELTA)


# ec2 = boto3.client('ec2',
#                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY'),
#                    aws_secret_access_key = os.environ.get('AWS_SECRET_KEY'),
#                    region_name="ap-south-1")
ec2_client = boto3.client('ec2',region_name = config.AWS_REGION)  # TODO auth using credentials

pod_data_provider = PodDataProvider(api, start_time=start, end_time=end, step=30)
node_data_provider = NodeDataProvider(api, start_time=start, end_time=end, step=30)

pod_data = pod_data_provider.get_data()
node_data = node_data_provider.get_data()

instance_types_list = get_instance_types_list(node_data)
ebs_bandwidths = get_ebs_bandwidths(ec2_client, instance_types_list)

bandwidth_map = get_network_bandwidth_map(config.NETWORK_BANDWIDTH_FILE_PATH)

pod_cpu_report = flag_pods_for_wrong_cpu_requests(pod_data, config.POD_REQUEST_MARGIN_FACTOR,
                                                  config.REQUEST_DIFFERENCE_THRESHOLD)
write_report("Pod CPU Report", pod_cpu_report, output_path_prefix + '/pod_cpu_report.csv')

pod_memory_report = flag_pods_for_wrong_memory_requests(pod_data, config.POD_REQUEST_MARGIN_FACTOR,
                                                        config.REQUEST_DIFFERENCE_THRESHOLD)
write_report("Pod Memory Report", pod_memory_report, output_path_prefix + "/pod_memory_request.csv")

wrong_pod_placement_report = flag_pods_by_wrong_node_placement_by_requests(pod_data, node_data,
                                                                           config.POD_SKEWNESS_THRESHOLD)
write_report("Wrong Pod placement report", wrong_pod_placement_report,
             output_path_prefix + "/wrong_pod_placement_report.csv")







bad_nodes_by_high_occurrence_of_high_cpu_usage = flag_nodes_by_high_probability_of_high_cpu_utilization(node_data,
                                                                                                        config.NODE_CPU_UTILIZATION_THRESHOLD,
                                                                                                        config.NODE_CPU_HIGH_UTIL_EXP_PROB)

node_pod_dict = group_pods_by_nodes(pod_data)

file = open(output_path_prefix + "/bad_node_by_cpu.csv", "w")
file.write("************Bad nodes by high occurrence of cpu > threshold************\n\n")
file.close()
bad_nodes_by_high_occurrence_of_high_cpu_usage.to_csv(output_path_prefix + "/bad_node_by_cpu.csv", mode='a',
                                                      index=False)
for node_name in bad_nodes_by_high_occurrence_of_high_cpu_usage['node name']:
    file = open(output_path_prefix + "/bad_node_by_cpu.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_cpu(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                        config.MAX_WINDOW_SIZE, config.NODE_CPU_UTILIZATION_THRESHOLD)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_cpu.csv", mode='a', index=False)


bad_nodes_by_high_avg_cpu_usage = flag_nodes_by_high_avg_cpu_utilization(node_data,
                                                                         config.NODE_CPU_UTILIZATION_THRESHOLD)

file = open(output_path_prefix + "/bad_node_by_cpu.csv", "a")
file.write("\n\n\n**********Bad nodes by high avg cpu usage***************\n\n")
file.close()
bad_nodes_by_high_avg_cpu_usage.to_csv(output_path_prefix + "/bad_node_by_cpu.csv", mode='a', index=False)
for node_name in bad_nodes_by_high_avg_cpu_usage['node name']:
    file = open(output_path_prefix + "/bad_node_by_cpu.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_cpu(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                        config.MAX_WINDOW_SIZE, config.NODE_CPU_UTILIZATION_THRESHOLD)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_cpu.csv", mode='a', index=False, header=False)




file = open(output_path_prefix + "/bad_node_by_memory.csv", "w")
file.write("************Bad nodes by high occurrence of memory > threshold************\n\n")
file.close()

bad_nodes_by_high_occurrence_of_high_memory_usage = flag_nodes_by_high_probability_of_high_memory_utilization(node_data,
                                                                                                              config.NODE_MEMORY_UTILIZATION_THRESHOLD,
                                                                                                              config.NODE_MEMORY_HIGH_UTIL_EXP_PROB)

bad_nodes_by_high_occurrence_of_high_memory_usage.to_csv(output_path_prefix + "/bad_node_by_memory.csv", mode='a',
                                                         index=False)
for node_name in bad_nodes_by_high_occurrence_of_high_memory_usage['node name']:
    file = open(output_path_prefix + "/bad_node_by_cpu.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_memory(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                           config.MAX_WINDOW_SIZE, config.NODE_MEMORY_UTILIZATION_THRESHOLD)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_memory.csv", mode='a', index=False)


bad_nodes_by_high_avg_memory_usage = flag_nodes_by_high_avg_cpu_utilization(node_data,
                                                                            config.NODE_MEMORY_UTILIZATION_THRESHOLD)
file = open(output_path_prefix + "/bad_node_by_memory.csv", "a")
file.write("\n\n\n**********Bad nodes by high avg memory usage***************\n\n")
file.close()
bad_nodes_by_high_avg_memory_usage.to_csv(output_path_prefix + "/bad_node_by_memory.csv", mode='a', index=False)
for node_name in bad_nodes_by_high_avg_memory_usage['node name']:
    file = open(output_path_prefix + "/bad_node_by_memory.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_memory(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                           config.MAX_WINDOW_SIZE, config.NODE_MEMORY_UTILIZATION_THRESHOLD)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_memory.csv", mode='a', index=False, header=False)


bad_nodes_by_high_occurrence_of_high_tx_bytes = flag_nodes_by_high_probability_of_high_network_tx_bytes(node_data,
                                                                                                        config.NODE_TX_BYTES_USAGE_THRESHOLD,
                                                                                                        config.NODE_NETWORK_BYTES_PROB_LIMIT,
                                                                                                        bandwidth_map)
file = open(output_path_prefix + "/bad_node_by_tx_bytes.csv", "w")
file.write("\n\n\n**********Bad nodes by high occurrence of tx bytes > threshold***************\n\n")
file.close()
bad_nodes_by_high_occurrence_of_high_tx_bytes.to_csv(output_path_prefix + "/bad_node_by_tx_bytes.csv", mode='a',
                                                     index=False)
for node_name in bad_nodes_by_high_occurrence_of_high_tx_bytes['node name']:
    file = open(output_path_prefix + "/bad_node_by_tx_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_tx_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                             config.MAX_WINDOW_SIZE, config.NODE_TX_BYTES_USAGE_THRESHOLD,
                                             bandwidth_map)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_tx_bytes.csv", mode='a', index=False, header=False)

bad_nodes_by_high_avg_tx_bytes = flag_nodes_by_high_avg_network_tx_bytes(node_data,
                                                                         config.NODE_TX_BYTES_USAGE_THRESHOLD,
                                                                         bandwidth_map)
file = open(output_path_prefix + "/bad_node_by_tx_bytes.csv", "a")
file.write("\n\n\n**********Bad nodes by high avg tx bytes***************\n\n")
file.close()
bad_nodes_by_high_avg_tx_bytes.to_csv(output_path_prefix + "/bad_node_by_tx_bytes.csv", mode='a', index=False)
for node_name in bad_nodes_by_high_avg_tx_bytes['node name']:
    file = open(output_path_prefix + "/bad_node_by_tx_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_tx_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                             config.MAX_WINDOW_SIZE, config.NODE_TX_BYTES_USAGE_THRESHOLD,
                                             bandwidth_map)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_tx_bytes.csv", mode='a', index=False, header=False)

bad_nodes_by_high_occurrence_of_high_rx_bytes = flag_nodes_by_high_probability_of_high_network_rx_bytes(node_data,
                                                                                                        config.NODE_RX_BYTES_USAGE_THRESHOLD,
                                                                                                        config.NODE_NETWORK_BYTES_PROB_LIMIT,
                                                                                                        bandwidth_map)
file = open(output_path_prefix + "/bad_node_by_rx_bytes.csv", "w")
file.write("\n\n\n**********Bad nodes by high occurrence of rx bytes > threshold***************\n\n")
file.close()
bad_nodes_by_high_occurrence_of_high_rx_bytes.to_csv(output_path_prefix + "/bad_node_by_rx_bytes.csv", mode='a',
                                                     index=False)
for node_name in bad_nodes_by_high_occurrence_of_high_rx_bytes['node name']:
    file = open(output_path_prefix + "/bad_node_by_rx_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_rx_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                             config.MAX_WINDOW_SIZE, config.NODE_RX_BYTES_USAGE_THRESHOLD,
                                             bandwidth_map)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_rx_bytes.csv", mode='a', index=False, header=False)

bad_nodes_by_high_avg_rx_bytes = flag_nodes_by_high_avg_network_rx_bytes(node_data,
                                                                         config.NODE_RX_BYTES_USAGE_THRESHOLD,
                                                                         bandwidth_map)
file = open(output_path_prefix + "/bad_node_by_rx_bytes.csv", "a")
file.write("\n\n\n**********Bad nodes by high avg rx bytes***************\n\n")
file.close()
bad_nodes_by_high_avg_rx_bytes.to_csv(output_path_prefix + "/bad_node_by_rx_bytes.csv", mode='a', index=False)
for node_name in bad_nodes_by_high_avg_rx_bytes['node name']:
    file = open(output_path_prefix + "/bad_node_by_rx_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_high_rx_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                             config.MAX_WINDOW_SIZE, config.NODE_RX_BYTES_USAGE_THRESHOLD,
                                             bandwidth_map)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_rx_bytes.csv", mode='a', index=False, header=False)



bad_nodes_by_high_occurrence_of_high_disk_bytes = flag_nodes_by_high_probability_of_high_disk_total_bytes(node_data,
                                                                                                          config.NODE_DISK_BYTES_USAGE_THRESHOLD,
                                                                                                          config.NODE_DISK_TOTAL_BYTES_PROB_LIMIT,
                                                                                                          ebs_bandwidths
                                                                                                          )
file = open(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", "w")
file.write("\n\n\n**********Bad nodes by high occurrence of disk total bytes > threshold***************\n\n")
file.close()
bad_nodes_by_high_occurrence_of_high_disk_bytes.to_csv(output_path_prefix + "/bad_node_by_disk_total_bytes.csv",
                                                       mode='a', index=False)
for node_name in bad_nodes_by_high_occurrence_of_high_disk_bytes['node name']:
    file = open(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_total_disk_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                                config.MAX_WINDOW_SIZE, config.NODE_DISK_BYTES_USAGE_THRESHOLD,
                                                ebs_bandwidths)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", mode='a', index=False, header=False)


bad_nodes_by_high_disk_bytes_avg = flag_nodes_by_high_avg_disk_total_bytes(node_data,
                                                                           config.NODE_DISK_BYTES_USAGE_THRESHOLD,
                                                                           ebs_bandwidths
                                                                           )
file = open(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", "w")
file.write("\n\n\n**********Bad nodes by high avg disk total bytes***************\n\n")
file.close()
bad_nodes_by_high_disk_bytes_avg.to_csv(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", mode='a', index=False)
for node_name in bad_nodes_by_high_disk_bytes_avg['node name']:
    file = open(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", "a")
    file.write(f"\n\nNode name: , {node_name}\n\n")
    file.close()
    df = mark_culprit_pods_for_total_disk_bytes(node_data[node_name], node_pod_dict[node_name], config.MIN_WINDOW_DIFF,
                                                config.MAX_WINDOW_SIZE, config.NODE_DISK_BYTES_USAGE_THRESHOLD,
                                                ebs_bandwidths)
    if df is not None:
        df.to_csv(output_path_prefix + "/bad_node_by_disk_total_bytes.csv", mode='a', index=False, header=False)


