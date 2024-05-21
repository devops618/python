# -- coding: utf-8 --
import requests
import json
import urllib3
import time
from datetime import datetime
from requests.auth import HTTPBasicAuth


# 时间date转long
def _datatolong(data_):
    datetime_obj = datetime.strptime(data_, "%Y-%m-%d %H:%M:%S")
    return int(
        time.mktime(datetime_obj.timetuple()) * 1000.0
        + datetime_obj.microsecond / 1000.0
    )


# elasticsearch 查询
def _search_kibana(now_time, start_time, es_query_client, client_id, es_index):
    urllib3.disable_warnings()  # 禁用SSL证书验证警告
    kibana_url = "http://ip/api/console/proxy"

    kibana_headers = {
        "content-type": "application/json",
        "kbn-xsrf": "true"
    }

    kibana_params = {
        "path": "%s*/_search" % es_index,  # 查询的索引
        "method": "GET"
    }

    kibana_data_body = {
        "query": {
            "bool": {
                "must": [
                    {"match_phrase": {"message": {"query": es_query_client}}},  # 查询的字符串
                    {"match_phrase": {"message": {"query": client_id}}},  # 查询的字符串，且的关系
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_time,
                                "lte": now_time,
                                "format": "epoch_millis"
                            }
                        }
                    }
                ]
            }
        }
    }

    kibana_result = requests.post(
        kibana_url, headers=kibana_headers, params=kibana_params, data=json.dumps(kibana_data_body), verify=False,
        auth=HTTPBasicAuth('username', 'password')
    )

    j = json.loads(kibana_result.text)
    ret = j['hits']['total']['value']
    return ret


def feishu_send(_time, es_query_client, mqtt_slave_count):
    url = "飞书机器人地址"
    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "过去5分钟emq客户端连接日志异常\n",
                    "content": [
                        [
                            {
                                "tag": "text",
                                "text": "查询时间: %s\n查询clientid: %s\n" % (_time, es_query_client),
                            },
                            {
                                "tag": "text",
                                "text": "prod-connector_new-logs索引日志数量: %s\n" % (mqtt_slave_count),
                            },
                            {
                                "tag": "at",
                                "user_id": "all"
                            }
                        ]
                    ]
                }
            }
        }
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, json=data)
    print(response.content)  # Print Response


if __name__ == "__main__":
    _time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    now_time = _datatolong(_time)  # 当前时间
    start_time = now_time - 300000  # 起始时间, now_time - 300000 表示5分钟前

    # mqtt_logs_index = "prod-emqtt-logs-"
    # mqtt_logs_client_id = "mqtt-connector-slave-"

    mqtt_slave_index = "prod-connector_new-logs-"
    mqtt_slave_client_id = "mosquitto_pub_test"

    es_query_client = "/clients/mosquitto_pub_test/"
    # mqtt_logs_count = _search_kibana(now_time, start_time, es_query_client, mqtt_logs_client_id, mqtt_logs_index)
    mqtt_slave_count = _search_kibana(now_time, start_time, es_query_client, mqtt_slave_client_id, mqtt_slave_index)

    if mqtt_slave_count != 60:
        feishu_send(_time, es_query_client, mqtt_slave_count)
