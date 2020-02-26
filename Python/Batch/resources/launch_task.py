# launch_container_task.py Code Sample
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import os

import azure.batch._batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

# Creates a container task in a known existing Azure Batch Service job.
# Uses environment variables for configuration


# Set up the configuration
batch_account_key = os.environ.get('BATCH_KEY')
batch_account_name = os.environ.get('BATCH_NAME')
batch_service_url = os.environ.get('BATCH_URL')
job_id = os.environ.get('BATCH_JOBID')
task_id = os.environ.get('BATCH_TASKID')
task_command_line = os.environ.get('BATCH_COMMANDLINE')

credentials = batchauth.SharedKeyCredentials(
    batch_account_name,
    batch_account_key)

batch_client = batch.BatchServiceClient(
    credentials=credentials,
    batch_url=batch_service_url)

# Retry 5 times -- default is 3
batch_client.config.retry_policy.retries = 5

task = batch.models.TaskAddParameter(
    id=task_id,
    command_line=task_command_line,
    container_settings=batch.models.TaskContainerSettings(image_name='ubuntu')
)

print("Creating the following task:\nBatch Account:\t" + batch_account_name + "\nJob ID:\t" + job_id)
print("Task:\t" + str(task.serialize()))
batch_client.task.add(job_id=job_id, task=task)
