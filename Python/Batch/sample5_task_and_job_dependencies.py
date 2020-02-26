# sample5_task_and_job_dependencies.py Code Sample
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

from __future__ import print_function
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import datetime
import os

import azure.storage.blob as azureblob
import azure.batch._batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

import common.helpers

_CONTAINER_NAME = 'poolsandresourcefiles'
_PREP_TASK_NAME = 'node_prep.bash'
_PREP_TASK_PATH = os.path.join('resources', _PREP_TASK_NAME)

_FINALIZE_TASK_NAME = 'launch_task.py'
_FINALIZE_TASK_PATH = os.path.join('resources', _FINALIZE_TASK_NAME)

def create_pool(batch_client, block_blob_client, pool_id, vm_size, vm_count):
    """Creates an Azure Batch pool with the specified id.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str pool_id: The id of the pool to create.
    :param str vm_size: vm size (sku)
    :param int vm_count: number of vms to allocate
    """
    
    image_ref_to_use = batchmodels.ImageReference(
        publisher='microsoft-azure-batch',
        offer='ubuntu-server-container',
        sku='16-04-lts',
        version='latest')

    sku_to_use = 'batch.node.ubuntu 16.04'

    container_conf = batchmodels.ContainerConfiguration(
        container_image_names=['ubuntu']
    )

    block_blob_client.create_container(
        _CONTAINER_NAME,
        fail_on_exist=False)

    sas_url = common.helpers.upload_blob_and_create_sas(
        block_blob_client,
        _CONTAINER_NAME,
        _PREP_TASK_NAME,
        _PREP_TASK_PATH,
        datetime.datetime.utcnow() + datetime.timedelta(hours=1))

    # Home directory for pool tasks is 
    pool_start_task_command_line= \
        '/bin/sh -c "chmod +x ./' + _PREP_TASK_NAME + \
            ' && ./' + _PREP_TASK_NAME + '" '

    pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            container_configuration=container_conf,
            node_agent_sku_id=sku_to_use,
            data_disks=[batchmodels.DataDisk(
                lun=0, # Use /dev/sdc when interacting with the scsi block device; /dev/sda is OS disk, /dev/sdb is temp disk
                caching='none',
                disk_size_gb=1023,
                storage_account_type='premium_lrs')]
            ),
        vm_size=vm_size,
        target_dedicated_nodes=vm_count,
        start_task=batchmodels.StartTask(
            command_line=pool_start_task_command_line,
            resource_files=[batchmodels.ResourceFile(
                file_path=_PREP_TASK_NAME,
                http_url=sas_url)],
            user_identity=batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                scope="pool",
                elevation_level="admin")
            ),
        )
    )

    common.helpers.create_pool_if_not_exist(batch_client, pool)


def submit_job1_and_add_tasks(batch_client, block_blob_client, job_id, pool_id):
    """Submits a job to the Azure Batch service and adds tasks 
    having dependencies on one another.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str job_id: The id of the job to create.
    :param str pool_id: The id of the pool to use.
    """

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id),
        uses_task_dependencies=True)
 
    batch_client.job.add(job)

    task_default_container_settings = batchmodels.TaskContainerSettings(
        image_name='ubuntu'
    )

    task_list = [
        batchmodels.TaskAddParameter(
            id="A_1",
            command_line='/bin/sh -c \"echo \'A_1 reporting in\'\"',
            container_settings=task_default_container_settings,
            exit_conditions=batchmodels.ExitConditions(
                pre_processing_error=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block),
                exit_codes=[
                    batchmodels.ExitCodeMapping(code=10,exit_options=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block)),
                    batchmodels.ExitCodeMapping(code=20,exit_options=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block))
                ],
                default=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.satisfy)
            )
        ),
        batchmodels.TaskAddParameter(
            id="B_1",
            command_line='/bin/sh -c \"echo \'B_1 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["A_1"])
        ),
        batchmodels.TaskAddParameter(
            id="C_1",
            command_line='/bin/sh -c \"echo \'C_1 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["B_1"])
        ),
        batchmodels.TaskAddParameter(
            id="Rain_1",
            command_line='/bin/sh -c \"echo \'Rain_1 reporting in\'\"',
            container_settings=task_default_container_settings
        ),
        batchmodels.TaskAddParameter(
            id="Sun_1",
            command_line='/bin/sh -c \"echo \'Sun_1 reporting in\'\"',
            container_settings=task_default_container_settings
        ),
        batchmodels.TaskAddParameter(
            id="Flowers_1",
            command_line='/bin/sh -c \"echo \'Flowers_1 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["Rain_1","Sun_1"])
        )
    ]

    batch_client.task.add_collection(job_id=job.id,value=task_list)


def submit_job2_and_add_tasks(batch_client, block_blob_client, job_id, pool_id):
    """Submits a job to the Azure Batch service and adds tasks 
    having dependencies on one another, including a dependency on a
    task that does not exist at time of creation.

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str job_id: The id of the job to create.
    :param str pool_id: The id of the pool to use.
    """

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id),
        uses_task_dependencies=True)
 
    batch_client.job.add(job)

    task_default_container_settings = batchmodels.TaskContainerSettings(
        image_name='ubuntu'
    )

    task_list = [
        batchmodels.TaskAddParameter(
            id="A_2",
            command_line='/bin/sh -c \"echo \'A_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            exit_conditions=batchmodels.ExitConditions(
                pre_processing_error=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block),
                exit_codes=[
                    batchmodels.ExitCodeMapping(code=10,exit_options=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block)),
                    batchmodels.ExitCodeMapping(code=20,exit_options=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.block))
                ],
                default=batchmodels.ExitOptions(dependency_action=batchmodels.DependencyAction.satisfy)
            ),
            depends_on=batchmodels.TaskDependencies(task_ids=["DelayedStart_1_2"])
        ),
        batchmodels.TaskAddParameter(
            id="B_2",
            command_line='/bin/sh -c \"echo \'B_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["A_2"])
        ),
        batchmodels.TaskAddParameter(
            id="C_2",
            command_line='/bin/sh -c \"echo \'C_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["B_2"])
        ),
        batchmodels.TaskAddParameter(
            id="Rain_2",
            command_line='/bin/sh -c \"echo \'Rain_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["A_2"])
        ),
        batchmodels.TaskAddParameter(
            id="Sun_2",
            command_line='/bin/sh -c \"echo \'Sun_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["A_2"])
        ),
        batchmodels.TaskAddParameter(
            id="Flowers_2",
            command_line='/bin/sh -c \"echo \'Flowers_2 reporting in\'\"',
            container_settings=task_default_container_settings,
            depends_on=batchmodels.TaskDependencies(task_ids=["Rain_2","Sun_2"])
        )
    ]

    batch_client.task.add_collection(job_id=job.id,value=task_list)


def finalize_job1(batch_client, block_blob_client, batch_account_name, batch_account_key, batch_service_url, job_id1, job_id2):
    """Adds a task to an existing Azure Batch service job1 to act as
    a finalization step. The task triggers submission of a simple operation task in job2,
    which will in turn trigger the rest of the tasks in job2 to run.
    

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str batch_account_name: The name of the Batch Account that will be used by the task added to job1.
    :param str batch_account_key: The shared auth key of the Batch Account that will be used by the task added to job1.
    :param str batch_service_url: The URL of the Batch Account that will be used by the task added to job1.
    :param str job_id1: The id of job1, which will execute this task.
    :param str job_id2: The id of job2, which will be triggered by finalization.
    """

    block_blob_client.create_container(
        _CONTAINER_NAME,
        fail_on_exist=False)

    sas_url = common.helpers.upload_blob_and_create_sas(
        block_blob_client,
        _CONTAINER_NAME,
        _FINALIZE_TASK_NAME,
        _FINALIZE_TASK_PATH,
        datetime.datetime.utcnow() + datetime.timedelta(hours=1))

    task_dependency_ids = []
    for deptask in batch_client.task.list(job_id=job_id1):
        task_dependency_ids.append(deptask.id)
    
    #task_command_line = '/bin/sh -i'
    task_command_line = '/bin/sh -c \'export PYTHONUSERBASE="$AZ_BATCH_TASK_WORKING_DIR" ' \
        + '&& pip -q --no-cache-dir install --no-warn-script-location azure-batch' \
        + '&& python $AZ_BATCH_TASK_WORKING_DIR/'+ _FINALIZE_TASK_NAME + '\''

    finalize_task = batchmodels.TaskAddParameter(
        id="Finalize_1_Trigger_2",
        container_settings=batchmodels.TaskContainerSettings(
            image_name="python:3.8.1"
        ),
        command_line=task_command_line,
        environment_settings=[
            batchmodels.EnvironmentSetting(name="BATCH_KEY",value=batch_account_key),
            batchmodels.EnvironmentSetting(name="BATCH_NAME",value=batch_account_name),
            batchmodels.EnvironmentSetting(name="BATCH_URL",value=batch_service_url),
            batchmodels.EnvironmentSetting(name="BATCH_JOBID",value=job_id2),
            batchmodels.EnvironmentSetting(name="BATCH_TASKID",value="DelayedStart_1_2"),
            batchmodels.EnvironmentSetting(name="BATCH_COMMANDLINE",value="/bin/sh -c \"echo \'DelayedStart_1_2 reporting in\'\""),
        ],
        depends_on=batchmodels.TaskDependencies(task_ids=task_dependency_ids),
        resource_files=[batchmodels.ResourceFile(
                file_path=_FINALIZE_TASK_NAME,
                http_url=sas_url)]
    )

    batch_client.task.add(job_id=job_id1,task=finalize_task)


def execute_sample(global_config, sample_config):
    """Executes the sample with the specified configurations.

    :param global_config: The global configuration to use.
    :type global_config: `configparser.ConfigParser`
    :param sample_config: The sample specific configuration to use.
    :type sample_config: `configparser.ConfigParser`
    """
    # Set up the configuration
    batch_account_key = global_config.get('Batch', 'batchaccountkey')
    batch_account_name = global_config.get('Batch', 'batchaccountname')
    batch_service_url = global_config.get('Batch', 'batchserviceurl')

    storage_account_key = global_config.get('Storage', 'storageaccountkey')
    storage_account_name = global_config.get('Storage', 'storageaccountname')
    storage_account_suffix = global_config.get(
        'Storage',
        'storageaccountsuffix')

    should_delete_container = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletecontainer')
    should_delete_job = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletejob')
    should_delete_pool = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletepool')
    pool_vm_size1 = sample_config.get(
        'DEFAULT',
        'poolvmsize1')
    pool_vm_count1 = sample_config.getint(
        'DEFAULT',
        'poolvmcount1')
    pool_vm_size2 = sample_config.get(
        'DEFAULT',
        'poolvmsize2')
    pool_vm_count2 = sample_config.getint(
        'DEFAULT',
        'poolvmcount2')

    # Print the settings we are running with
    common.helpers.print_configuration(global_config)
    common.helpers.print_configuration(sample_config)

    credentials = batchauth.SharedKeyCredentials(
        batch_account_name,
        batch_account_key)
    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    # Retry 5 times -- default is 3
    batch_client.config.retry_policy.retries = 5

    block_blob_client = azureblob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key,
        endpoint_suffix=storage_account_suffix)

    job_id1 = common.helpers.generate_unique_resource_name(
        "PoolsAndResourceFilesJob1")
    job_id2 = common.helpers.generate_unique_resource_name(
        "PoolsAndResourceFilesJob2")
    pool_id1 = "PoolsAndResourceFilesPool1-" + os.path.splitext(os.path.basename(__file__))[0]
    pool_id2 = "PoolsAndResourceFilesPool2-" + os.path.splitext(os.path.basename(__file__))[0]

    try:
        create_pool(
            batch_client,
            block_blob_client,
            pool_id1,
            pool_vm_size1,
            pool_vm_count1)

        create_pool(
            batch_client,
            block_blob_client,
            pool_id2,
            pool_vm_size2,
            pool_vm_count2)

        submit_job1_and_add_tasks(
            batch_client,
            block_blob_client,
            job_id1, pool_id1)

        submit_job2_and_add_tasks(
            batch_client,
            block_blob_client,
            job_id2, pool_id2)

        finalize_job1(
            batch_client, 
            block_blob_client, 
            batch_account_name, 
            batch_account_key, 
            batch_service_url, 
            job_id1, 
            job_id2)

        common.helpers.wait_for_tasks_to_complete(
            batch_client,
            job_id2,
            datetime.timedelta(minutes=25))

        tasks = batch_client.task.list(job_id1)
        task_ids = [task.id for task in tasks]

        common.helpers.print_task_output(batch_client, job_id1, task_ids)
        
        tasks = batch_client.task.list(job_id2)
        task_ids = [task.id for task in tasks]

        common.helpers.print_task_output(batch_client, job_id2, task_ids)
    finally:
        # clean up
        if should_delete_container:
            block_blob_client.delete_container(
                _CONTAINER_NAME,
                fail_not_exist=False)
        if should_delete_job:
            print("Deleting job: ", job_id1)
            batch_client.job.delete(job_id1)
            print("Deleting job: ", job_id2)
            batch_client.job.delete(job_id2)
        if should_delete_pool:
            print("Deleting pool: ", pool_id1)
            batch_client.pool.delete(pool_id1)
            print("Deleting pool: ", pool_id2)
            batch_client.pool.delete(pool_id2)


if __name__ == '__main__':
    global_config = configparser.ConfigParser()
    global_config.read(common.helpers._SAMPLES_CONFIG_FILE_NAME)

    sample_config = configparser.ConfigParser()
    sample_config.read(
        os.path.splitext(os.path.basename(__file__))[0] + '.cfg')

    execute_sample(global_config, sample_config)
