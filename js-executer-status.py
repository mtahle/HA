import docker
import boto3
import os

cloudwatch = boto3.client('cloudwatch')

client = docker.from_env()

megabytes_values = {'mem_current', 'mem_total'}
percentage_values = {'cpu_percent', 'mem_percent'}
numbers_unit = {'status'}


def calculate_stats_summary(stats):
    name = stats['name'].encode("utf-8")
    name = name.strip('/')
    mem_current = float(stats["memory_stats"]["usage"]/1024/1024/1024)
    mem_total = float(stats["memory_stats"]["limit"]/1024/1024/1024)
    mem_percent = round(float((mem_current / mem_total) * 100.0), 2)

    # print('Container name: {}, mem_percent: {} %, mem_total: {} GiB, mem_current:{} GiB'.format(name,
    #     mem_percent, mem_total, mem_current))
    cpu_count = len(stats["cpu_stats"]["cpu_usage"]["percpu_usage"])
    cpu_percent = 0.0
    cpu_delta = float(stats["cpu_stats"]["cpu_usage"]["total_usage"]) - \
        float(stats["precpu_stats"]["cpu_usage"]["total_usage"])
    system_delta = float(stats["cpu_stats"]["system_cpu_usage"]) - \
        float(stats["precpu_stats"]["system_cpu_usage"])
    if system_delta > 0.0:
        cpu_percent = cpu_delta / system_delta * 100.0 * cpu_count
    summary_stats = {'cpu_percent': cpu_percent,
                     'mem_current': mem_current, 'mem_percent': mem_percent, 'name': name}
    return summary_stats


client = docker.from_env()

for container in client.containers.list(all=True):
    check_string = 'php'
    if check_string in container.name:
        if container.status == 'running':
            status = 1
        elif container.status == 'exited':
            status = 0
        try:
            cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': "Status",
                        'Dimensions': [
                            {
                                'Name': 'Services Name',
                                'Value': container.name
                            },
                            {
                                'Name': 'Host Name',
                                'Value': os.uname()[1]
                            },

                        ],
                        'Unit': 'None',
                        'Value': float(status)

                    },

                ],
                Namespace='Docker/Stats')

        except ValueError:
                # print('not convertable key {}, value is: {}'.format(key, value))
            continue
    try:
        full_stats = container.stats(stream=False)
        summary_stats = calculate_stats_summary(full_stats)
        for key in summary_stats:
            try:
                if key in percentage_values:
                    unit = 'Percent'
                elif key in megabytes_values:
                    unit = 'Megabytes'
                else:
                    unit = 'None'
                cloudwatch.put_metric_data(
                    MetricData=[
                        {
                            'MetricName': key,
                            'Dimensions': [
                                {
                                    'Name': 'Services Name',
                                    'Value': '{}'.format(summary_stats['name'])
                                },
                                {
                                    'Name': 'Host Name',
                                    'Value': os.uname()[1]
                                },
                            ],
                            'Unit': unit,
                            'Value': float(summary_stats[key])

                        },
                    ],
                    Namespace='Docker/Stats')

            except ValueError:
                # print('not convertable key {}, value is: {}'.format(key, value))
                continue

    except KeyError:
        continue
# print('Number of caontainers: {}'.format(len(client.containers.list(all=True,filters={"name":"tb-js-executor"}))))
