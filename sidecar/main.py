import os
import logging
from typing import Optional

import requests
from pydantic import BaseModel
from kubernetes import client, config, watch


namespace = "default"
solution_file = "/src/solution.txt"
errors_file = "/src/errors.txt"

solver_container_index = 0  # The index of the container in the pod's container list

solution_service = "solution-service"

computation_id = os.environ["COMPUTATION_ID"]
user_id = os.environ["USER_ID"]

logging.basicConfig(level=logging.INFO)


class SolutionInformationInput(BaseModel):
    user_id: str
    computation_id: str
    status: Optional[str]
    reason: Optional[str]
    solver: Optional[str]
    body: Optional[str]


def save_solution(pod):
    logging.info("Detected solver finish")
    solution_input = SolutionInformationInput(user_id=user_id, computation_id=computation_id)

    if os.path.exists(errors_file) and os.stat(errors_file).st_size != 0:
        with open(errors_file, "r") as fd:
            solution_input.status = "Failure"
            solution_input.reason = "Minizinc Error"
            solution_input.body = fd.read()

    elif os.path.exists(solution_file):
        with open(solution_file, "r") as fd:
            solution_input.status = "Success"
            solution_input.body = fd.read()

    solution_input.solver = pod.spec.containers[solver_container_index].image

    headers = {'UserId': 'system', 'Role': 'admin'}
    response = requests.post('http://'+solution_service+'/api/solutions/upload', json=solution_input.dict(), headers=headers)
    if response.status_code != 200:
        logging.error("solution-service replied {}".format(response.text) +
                      "On request {}".format(solution_input.dict()))


def solver_terminated(pod: client.V1Pod):
    return pod.status.container_statuses[solver_container_index].state.terminated


def main():
    config.load_incluster_config()
    core_api = client.CoreV1Api()

    pod_name = os.environ["HOSTNAME"]
    field_selector = "metadata.name={}".format(pod_name)
    response = core_api.list_namespaced_pod(namespace, field_selector=field_selector)
    pod = response.items[0]

    if solver_terminated(pod):
        save_solution(pod)
        exit()

    w = watch.Watch()

    for event in w.stream(core_api.list_namespaced_pod, field_selector=field_selector, namespace=namespace, _continue=response.metadata._continue):
        pod = event["object"]
        if solver_terminated(pod):
            save_solution(pod)
            w.stop()
            exit()


if __name__ == "__main__":
    main()
