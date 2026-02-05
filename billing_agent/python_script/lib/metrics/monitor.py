"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import datetime

from google.cloud import monitoring_v3
from pytz import timezone


def nat_request(date_str: str, projects: [str]) -> dict:
    """

    :param date_str:
    :param projects:
    :return:
    """
    nat_result = {}

    # e.g. 2022-01-01 --> 2022-01-01 23:59:59 PST
    datetime_pst8pdt = datetime.datetime.strptime(f'{date_str} 23:59:59', "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone('PST8PDT'))
    # print(datetime_pst8pdt.strftime("%Y-%m-%d %H:%M:%S %Z"))
    # print(datetime_pst8pdt.timestamp())
    ts = datetime_pst8pdt.timestamp()
    seconds = int(ts)
    nanos = int((ts - seconds) * 10 ** 9)

    # result interval, end_time=start_time, there will be only 1 time series in result.
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": seconds, "nanos": nanos},
        }
    )

    # result aggregation, though there will be only 1 time series in result.
    # the result value will be the SUM of past 1 day (86399 seconds)
    # e.g. sum over 2022-01-01 00:00:00 PST to 2022-01-01 23:59:59 PST
    # the result will be further grouped by project_id, region, instance_name etc.

    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": 86399},  # 1 day
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            "group_by_fields": [
                'metric.nat_gateway_name',
                'resource.project_id',
                'metadata.user_labels.inner_ip',
                'metadata.user_labels.psm',
                'metadata.system_labels.region',
                'metadata.user_labels.instance_name',
            ],
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM
        }
    )
    for project in projects:
        nat_request_single_project(interval, aggregation, f'projects/{project}', nat_result)
    return nat_result


def nat_request_single_project(interval: monitoring_v3.types.TimeInterval,
                               aggregation: monitoring_v3.types.Aggregation,
                               project: str,
                               result: dict) -> None:
    nat_received = nat_received_single_project(interval, aggregation, project)
    nat_sent = nat_sent_single_project(interval, aggregation, project)
    for received_result in nat_received.pages:
        for timeseries in received_result.time_series:
            # print(timeseries)
            time_series_dict = {
                "project_id": timeseries.resource.labels["project_id"],
                "nat_gateway_name": timeseries.metric.labels["nat_gateway_name"],
                "region": timeseries.metadata.system_labels.fields["region"].string_value,
                "psm": timeseries.metadata.user_labels["psm"],
                "instance_name": timeseries.metadata.user_labels["instance_name"],
                "inner_ip": timeseries.metadata.user_labels["inner_ip"],
                "received_bytes_count": timeseries.points[0].value.int64_value / pow(2, 30),
                "sent_bytes_count": 0,
                "usage_date": str(timeseries.points[0].interval.start_time.date())
            }
            key = '-'.join([
                timeseries.metric.labels["nat_gateway_name"],
                timeseries.metadata.system_labels.fields["region"].string_value,
                timeseries.metadata.user_labels["psm"],
                timeseries.metadata.user_labels["instance_name"],
                timeseries.metadata.user_labels["inner_ip"],
            ])
            result[key] = time_series_dict

    for sent_result in nat_sent.pages:
        for timeseries in sent_result.time_series:
            time_series_dict = {
                "project_id": timeseries.resource.labels["project_id"],
                "nat_gateway_name": timeseries.metric.labels["nat_gateway_name"],
                "region": timeseries.metadata.system_labels.fields["region"].string_value,
                "psm": timeseries.metadata.user_labels["psm"],
                "instance_name": timeseries.metadata.user_labels["instance_name"],
                "inner_ip": timeseries.metadata.user_labels["inner_ip"],
                "received_bytes_count": 0,
                "sent_bytes_count": timeseries.points[0].value.int64_value / pow(2, 30),
                "usage_date": str(timeseries.points[0].interval.start_time.date())
            }
            key = '-'.join([
                timeseries.metric.labels["nat_gateway_name"],
                timeseries.metadata.system_labels.fields["region"].string_value,
                timeseries.metadata.user_labels["psm"],
                timeseries.metadata.user_labels["instance_name"],
                timeseries.metadata.user_labels["inner_ip"],
            ])
            if result[key]:
                received_dict = result[key]
                received_dict["sent_bytes_count"] = timeseries.points[0].value.int64_value / pow(2, 30)
                result[key] = received_dict
            else:
                result[key] = time_series_dict


def nat_received_single_project(interval: monitoring_v3.types.TimeInterval,
                                aggregation: monitoring_v3.types.Aggregation,
                                project: str) -> monitoring_v3.services.metric_service.pagers.ListTimeSeriesPager:
    client = monitoring_v3.MetricServiceClient()
    results = client.list_time_series(
        request={
            "name": project,
            "filter": """
                    metric.type = "compute.googleapis.com/nat/received_bytes_count" AND
                    resource.type = "gce_instance" 
                """,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": aggregation,
        }
    )
    return results


def nat_sent_single_project(interval: monitoring_v3.types.TimeInterval,
                            aggregation: monitoring_v3.types.Aggregation,
                            project: str) -> monitoring_v3.services.metric_service.pagers.ListTimeSeriesPager:
    client = monitoring_v3.MetricServiceClient()
    results = client.list_time_series(
        request={
            "name": project,
            "filter": """
                metric.type = "compute.googleapis.com/nat/sent_bytes_count" AND
                resource.type = "gce_instance" 
            """,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": aggregation,
        }
    )
    return results


def object_count(date_str: str, project: str, bucket_name: str) -> int:
    project_name = f'projects/{project}'
    datetime_pst8pdt = datetime.datetime.strptime(f'{date_str} 23:59:59', "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone('PST8PDT'))
    # print(datetime_pst8pdt.strftime("%Y-%m-%d %H:%M:%S %Z"))
    # print(datetime_pst8pdt.timestamp())
    ts = datetime_pst8pdt.timestamp()
    seconds = int(ts)
    nanos = int((ts - seconds) * 10 ** 9)

    # result interval, end_time=start_time, there will be only 1 time series in result.
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": seconds, "nanos": nanos},
        }
    )

    # result aggregation, though there will be only 1 time series in result.
    # the result value will be the SUM of past 1 day (86399 seconds)
    # e.g. sum over 2022-01-01 00:00:00 PST to 2022-01-01 23:59:59 PST
    # the result will be further grouped by project_id, region, instance_name etc.

    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": 86399},  # 1 day
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            "group_by_fields": [
                'resource.project_id',
                'metadata.user_labels.bucket_name',
                'resource.location',
                'metadata.user_labels.storage_class'
            ],
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_MEAN
        }
    )

    client = monitoring_v3.MetricServiceClient()
    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": f"""
                metric.type = "storage.googleapis.com/storage/object_count" AND
                resource.type = "gcs_bucket"  AND
                metadata.user_labels.bucket_name = "{bucket_name}"
            """,
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": aggregation,
        }
    )
    for result in results.pages:
        for timeseries in result.time_series:
            time_series_dict = {
                'bucket_name': timeseries.metadata.user_labels['bucket_name'],
                'storage_class': timeseries.metadata.user_labels['storage_class'],
                'region': timeseries.resource.labels['location'],
                'object_count': timeseries.points[0].value.double_value
            }
            print(time_series_dict)
