import argparse
import os
import pprint
import random
import time
import sys

from google.cloud import monitoring_v3

# projectid
project_id = sys.argv[1]
queue = sys.argv[2]

# Avoid collisions with other runs
# RANDOM_SUFFIX = str(random.randint(1000, 9999))

# create custom metric descriptor 
client = monitoring_v3.MetricServiceClient()
project_name = client.project_path(project_id)
descriptor = monitoring_v3.types.MetricDescriptor()
descriptor.type = 'custom.googleapis.com/' + queue
descriptor.metric_kind = (
    monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
descriptor.value_type = (
    monitoring_v3.enums.MetricDescriptor.ValueType.DOUBLE)
descriptor.description = 'This is a simple example of a custom metric.'
descriptor = client.create_metric_descriptor(project_name, descriptor)
print('Created {}.'.format(descriptor.name))

# Writing Metirc Data 
series = monitoring_v3.types.TimeSeries()
series.metric.type = 'custom.googleapis.com/' + queue + RANDOM_SUFFIX
series.resource.type = 'global'
series.resource.labels['project_id'] = project_name
point = series.points.add()
point.value.double_value = 3.14
now = time.time()
point.interval.end_time.seconds = int(now)
point.interval.end_time.nanos = int(
    (now - point.interval.end_time.seconds) * 10**9)
client.create_time_series(project_name, [series])
