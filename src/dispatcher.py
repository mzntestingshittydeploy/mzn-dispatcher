import os
import shlex
from uuid import uuid4
from typing import Dict, List

from kubernetes_asyncio import client

from .job import Job


class Dispatcher:
    batch_api: client.BatchV1Api

    def __init__(self, batch_api: client.BatchV1Api):
        self.batch_api = batch_api
        self.job_prefix = os.environ["JOB_PREFIX"]
        self.sidecar_image = os.environ["SIDECAR_IMAGE_NAME"]

    async def start_job(self, image: str, option_string: str, model_url: str, data_url: str, cpu_request: int,
                        mem_request: int, timeout_seconds: int, labels: Dict[str, str] = {}) -> Job:
        labels["app"] = self.job_prefix
        name = self.job_prefix + "-" + str(uuid4())

        cpu_request_str = str(cpu_request)
        mem_request_str = str(mem_request)+"Mi"

        # Configure shared volume for input files
        volume = client.V1Volume(
                name="src-dir",
                empty_dir=client.V1EmptyDirVolumeSource()
                )

        # Configure solver container
        resources = client.V1ResourceRequirements(limits={"cpu": cpu_request_str, "memory": mem_request_str},
                                                  requests={"cpu": cpu_request_str, "memory": mem_request_str})

        options = list(map(shlex.quote, shlex.split(option_string)))  # Split and escape option string
        options += ["-p", cpu_request_str, "/src/model.mzn", "/src/data.dzn"]
        mzn_string = " ".join(["minizinc"] + options + [">", "/src/solution.txt", "2>", "/src/errors.txt"])
        solver = client.V1Container(
                name=name,
                image=image,
                command=["sh", "-c", mzn_string],
                resources=resources,
                volume_mounts=[client.V1VolumeMount(name="src-dir",
                                                    mount_path="/src")])

        # Configure sidecar container to watch the solver
        sidecar = client.V1Container(
                name="sidecar-"+name,
                image=self.sidecar_image,
                image_pull_policy="IfNotPresent",
                env=[client.V1EnvVar(name="COMPUTATION_ID", value=labels["computation_id"]),
                     client.V1EnvVar(name="USER_ID", value=labels["user_id"])],
                volume_mounts=[client.V1VolumeMount(name="src-dir",
                                                    mount_path="/src")])

        # Configure initContainer
        env = [client.V1EnvVar(name="MODEL_URL", value=model_url)]
        if data_url:
            env.append(client.V1EnvVar(name="DATA_URL", value=data_url))

        # If the second wget fails, because DATA_URL isn't defined, we create an empty /src/data.dzn
        # TODO probably not ideal
        wget = 'wget -O /src/model.mzn "$MODEL_URL" && ( wget -O /src/data.dzn "$DATA_URL" || touch /src/data.dzn )'
        initContainer = client.V1Container(
                name="init-"+name,
                image="busybox",
                command=["sh", "-c", wget],
                env=env,
                volume_mounts=[client.V1VolumeMount(name="src-dir",
                                                    mount_path="/src")])

        # Create and configurate a spec section
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": name}),
            spec=client.V1PodSpec(restart_policy="Never",
                                  containers=[solver, sidecar],
                                  init_containers=[initContainer],
                                  volumes=[volume]))

        # Create the specification of deployment
        spec = client.V1JobSpec(
            template=template,
            active_deadline_seconds=timeout_seconds,
            backoff_limit=4)

        # Instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=name, labels=labels),
            spec=spec)

        started_job = await self.batch_api.create_namespaced_job(
            body=job,
            namespace="default")
        return Job(started_job, self.batch_api)

    async def get_jobs(self, labels: Dict[str, str] = {}) -> List[Job]:
        labels["app"] = self.job_prefix
        label_selector = Dispatcher._labels_to_string(labels)

        v1jobs = await self.batch_api.list_namespaced_job(namespace="default", label_selector=label_selector)
        return [Job(j, self.batch_api) for j in v1jobs.items]

    @staticmethod
    def _labels_to_string(labels: Dict[str, str]) -> str:
        return ",".join([label+"="+value for label, value in labels.items()])
