import os.path
from datetime import datetime

import requests
import pandas as pd

import lark_oapi as lark
from lark_oapi.api.drive.v1 import *


def get_file_token():
    # 创建client
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    file = open(file_path, "rb")
    request: UploadAllMediaRequest = UploadAllMediaRequest.builder() \
        .request_body(UploadAllMediaRequestBody.builder()
                      .file_name(file_name)
                      .parent_type("ccm_import_open")
                      .extra('{ "obj_type": "sheet","file_extension": "xlsx"}')  # 把本地xlsx导入云文档sheet
                      .size(file_size)
                      .file(file)
                      .build()) \
        .build()

    # 发起请求
    response: UploadAllMediaResponse = client.drive.v1.media.upload_all(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.drive.v1.media.upload_all failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))

    return response.data.file_token


def get_ticket():
    # 创建client
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: CreateImportTaskRequest = CreateImportTaskRequest.builder() \
        .request_body(ImportTask.builder()
                      .file_extension("xlsx")
                      .file_token(file_token)
                      .type("sheet")
                      .point(ImportTaskMountPoint.builder()
                             .mount_type(1)  # 云空间挂载点
                             .mount_key(folder_id)  # 如为空，则挂载到导入者的云空间根目录下
                             .build())
                      .build()) \
        .build()

    # 发起请求
    response: CreateImportTaskResponse = client.drive.v1.import_task.create(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.drive.v1.import_task.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))

    return response.data.ticket


def get_result():
    # 创建client
    client = lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: GetImportTaskRequest = GetImportTaskRequest.builder() \
        .ticket(ticket) \
        .build()

    # 发起请求
    response: GetImportTaskResponse = client.drive.v1.import_task.get(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.drive.v1.import_task.get failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    APP_ID = "APP_ID"
    APP_SECRET = "APP_SECRET"
    folder_id = "folder_id"

    url = "http://ip/marathon/v2/apps"
    time_now = datetime.now().strftime("%Y%m%d")
    file_name = "微服务信息" + "_" + time_now + ".xlsx"
    file_path = "./xlsx/" + file_name

    x = requests.get(url)

    service_dict = {}
    for service in x.json()['apps']:
        try:
            vhost = service['labels']['HAPROXY_0_VHOST']
            java_ops = service['env']['JAVA_OPTS']
        except Exception as e:
            vhost = ""
            java_ops = ""
        service_dict[service['id']] = [service['instances'], service['cpus'], service['mem'], vhost, service['version'],
                                       service['container']['docker']['image'], service['tasksStaged'],
                                       service['tasksRunning'], service['tasksHealthy'], service['tasksUnhealthy'],
                                       service['container']['docker']['parameters'], java_ops]

    columns = ['instances', 'cpus', 'mem', 'HAPROXY_0_VHOST', 'version', 'image', 'tasksStaged', 'tasksRunning',
               'tasksHealthy', 'tasksUnhealthy', 'parameters', 'JAVA_OPTS']

    df = pd.DataFrame(service_dict).T
    df.columns = columns

    df.to_excel(file_path)

    file_size = os.path.getsize(file_path)

    file_token = get_file_token()
    ticket = get_ticket()
    get_result()
