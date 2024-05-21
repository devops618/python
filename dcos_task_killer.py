# -- coding: utf-8 --
import sys
import requests

mesos_tasks_url = "http://ip:5050/tasks"
marathon_tasks_url = "http://ip/service/marathon/v2/tasks"
framework_id = "framework_id"

query_parameters = {"framework_id": framework_id, "limit": "100000"}
x = requests.get(mesos_tasks_url, params=query_parameters)

mesos_tasks_dict = {}
for task in x.json()['tasks']:
    if task['state'] == "TASK_RUNNING":
        if task['container']['type'] == "DOCKER":
            task_id = task['id']
            image = task['container']['docker']['image']
            for state in task['statuses']:
                if state['state'] == "TASK_RUNNING":
                    container_id = "mesos-" + state['container_status']['container_id']['value']
                    mesos_tasks_dict[task_id] = {"container_id": container_id, "image": image}

y = requests.get(marathon_tasks_url)

marathon_tasks_dict = {}
for task in y.json()['tasks']:
    app_id = task['appId']
    host = task['host']
    task_id = task['id']
    marathon_tasks_dict[task_id] = {"app_id": app_id, "host": host}

tasks_tmp = {}
tasks_dict = {}
for task_id in mesos_tasks_dict:
    if task_id in marathon_tasks_dict:
        tasks_tmp.update(mesos_tasks_dict[task_id])
        tasks_tmp.update(marathon_tasks_dict[task_id])
        tasks_dict[task_id] = tasks_tmp
        tasks_tmp = {}

app_id = sys.argv[0]
image = sys.argv[1]

tasks_killer_dict = {}
for task_id in tasks_dict:
    if tasks_dict[task_id]['app_id'] == app_id and tasks_dict[task_id]['image'] == image:
        tasks_killer_dict[task_id] = tasks_dict[task_id]

for item in tasks_killer_dict.values():
    print("ssh %s 'docker stop %s'" %(item['host'], item['container_id']))
