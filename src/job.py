from __future__ import annotations  # https://stackoverflow.com/questions/41135033/type-hinting-within-a-class

import logging

from kubernetes_asyncio import client

from .models import Solver, SolverStatus


class Job(object):

    """
    A wrapper around the API-servers' job object
    """

    _job: client.V1Job
    _batch_api: client.BatchV1Api

    def __init__(self, job: client.V1Job, batch_api: client.BatchV1Api) -> None:
        self._job: client.V1Job = job
        self._batch_api: client.BatchV1Api = batch_api

    @property
    def name(self) -> str:
        return self._job.metadata.name

    @property
    def image(self) -> str:
        return self._job.spec.template.spec.containers[0].image

    @property
    def cpu_request(self) -> int:
        cpu_request_str = self._job.spec.template.spec.containers[0].resources.requests["cpu"]
        return int(cpu_request_str)

    @property
    def mem_request(self) -> int:
        mem_request_str = self._job.spec.template.spec.containers[0].resources.requests["memory"]
        return int(mem_request_str[:-2]) # Truncate the "Mi" postfix

    @property
    def active(self) -> int:
        return self._job.status.to_dict().get('active', 0) or 0

    @property
    def succeeded(self) -> int:
        return self._job.status.to_dict().get('succeeded', 0) or 0

    @property
    def failed(self) -> int:
        return self._job.status.to_dict().get('failed', 0) or 0

    def get_solver_representation(self) -> Solver:
        solver_status = SolverStatus(active=self.active,
                                     succeeded=self.succeeded,
                                     failed=self.failed)
        return Solver(image=self.image,
                      cpu_request=self.cpu_request,
                      mem_request=self.mem_request,
                      status=solver_status)

    async def update_status(self) -> None:
        api_response = await self._batch_api.read_namespaced_job_status(
            name=self.name,
            namespace="default")

        logging.debug(api_response.status.to_dict())

        self._job = api_response

    def get_output(self) -> str:
        return "placeholder"

    async def delete(self) -> str:
        api_response = await self._batch_api.delete_namespaced_job(
            name=self.name,
            namespace="default",
            body=client.V1DeleteOptions(
                propagation_policy='Background')
            )
        return api_response.status + " " + (api_response.reason or "")
