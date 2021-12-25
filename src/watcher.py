import os
import logging
import asyncio
from typing import Dict

from kubernetes_asyncio import client, watch
import aiohttp

from .dispatcher import Dispatcher
from .models import FinishComputationMessage


class Watcher:

    batch_api: client.BatchV1Api
    dispatcher: Dispatcher
    scheduler_session: aiohttp.ClientSession
    job_prefix: str
    hostname: str

    def __init__(self, batch_api: client.BatchV1Api, dispatcher: Dispatcher, scheduler_session):
        self.batch_api = batch_api
        self.dispatcher = dispatcher
        self.scheduler_session = scheduler_session
        self.job_prefix = os.environ["JOB_PREFIX"]
        self.scheduler_name = os.environ["SCHEDULER_NAME"]
        self.hostname = os.environ["HOSTNAME"]

    async def watch_jobs(self, computation_id: str, user_id: str):  # TODO variable namespace
        namespace = "default"
        label_selector = self._labels_to_string({"app": self.job_prefix, "computation_id": computation_id})
        response = await self.batch_api.list_namespaced_job(namespace="default", label_selector=label_selector)

        num_jobs = len(response.items)

        successful = 0
        failed = 0
        for job in response.items:
            successful, failed = self.update_counts(job, successful, failed)
            if successful > 0 or failed >= num_jobs:
                await self.handle_termination(computation_id, user_id)
                return

        w = watch.Watch()

        async for event in w.stream(self.batch_api.list_namespaced_job,
                                    namespace=namespace, label_selector=label_selector,
                                    _continue=response.metadata._continue):
            job = event["object"]

            successful, failed = self.update_counts(job, successful, failed)
            if successful > 0 or failed >= num_jobs or event["type"] == "DELETED":
                await self.handle_termination(computation_id, user_id)
                return

    def update_counts(self, job, successful, failed):
        if (job.status.succeeded or 0) > 0:
            successful += 1
        elif (job.status.failed or 0) > 0:
            failed += 1

        return (successful, failed)

    async def handle_termination(self, computation_id: str, user_id: str):

        await self.notify_scheduler(computation_id, user_id)

        jobs = await self.dispatcher.get_jobs(labels={"computation_id": computation_id})
        for job in jobs:
            await job.delete()

    async def notify_scheduler(self, computation_id: str, user_id: str):
        url = "http://{name}/api/scheduler/finish_computation".format(name=self.scheduler_name)
        data = FinishComputationMessage(computation_id=computation_id, user_id=user_id).dict()
        headers = {'UserId': 'system', 'Role': 'admin'}

        for _ in range(5):
            try:
                async with self.scheduler_session.post(url, json=data, headers=headers) as response:
                    status = response.status

                    if status == 200:
                        break
                    else:
                        logging.error('Scheduler Service replied with error code: {}'.format(status) +
                                      ' and message {}'.format(await response.text()))

            except aiohttp.client_exceptions.ClientConnectorError:
                logging.error('Failed to contact scheduler with domain name "{}" '.format(self.scheduler_name))

            await asyncio.sleep(5)

        else:
            logging.error('Can not notify "{}". Cleaning up resources anyways.'.format(self.scheduler_name))
            return False

        return True

    def _labels_to_string(self, labels: Dict[str, str]) -> str:
        return ",".join([label+"="+value for label, value in labels.items()])

    async def schutdown(self):
        await self.scheduler_session.close()
