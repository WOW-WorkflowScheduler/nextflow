/*
 * Copyright 2022, Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nextflow.cloud.google.batch

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem
import nextflow.cloud.google.batch.client.BatchConfig
import nextflow.processor.TaskBean
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GoogleBatchTaskHandlerTest extends Specification {

    def 'should create submit request/1' () {
        given:
        def WORK_DIR = CloudStorageFileSystem.forBucket('foo').getPath('/scratch')
        def CONTAINER_IMAGE = 'debian:latest'
        def exec = Mock(GoogleBatchExecutor) {
            getConfig() >> Mock(BatchConfig)
        }
        and:
        def bean = new TaskBean(workDir: WORK_DIR, inputFiles: [:])
        def task = Mock(TaskRun) {
            toTaskBean() >> bean
            getHashLog() >> 'abcd1234'
            getWorkDir() >> WORK_DIR
            getContainer() >> CONTAINER_IMAGE
            getConfig() >> Mock(TaskConfig) {
                getCpus() >> 2
            }
        }

        and:
        def handler = new GoogleBatchTaskHandler(task, exec)

        when:
        def req = handler.newSubmitRequest(task)
        then:
        def taskGroup = req.getTaskGroups(0)
        def runnable = taskGroup.getTaskSpec().getRunnables(0)
        def instancePolicy = req.getAllocationPolicy().getInstances(0).getPolicy()
        and:
        taskGroup.getTaskSpec().getComputeResource().getCpuMilli() == 2_000
        taskGroup.getTaskSpec().getComputeResource().getMemoryMib() == 0
        taskGroup.getTaskSpec().getMaxRunDuration().getSeconds() == 0
        and:
        runnable.getContainer().getCommandsList().join(' ') == '/bin/bash -o pipefail -c trap "{ cp .command.log /mnt/foo/scratch/.command.log; }" ERR; /bin/bash /mnt/foo/scratch/.command.run 2>&1 | tee .command.log'
        runnable.getContainer().getImageUri() == CONTAINER_IMAGE
        runnable.getContainer().getOptions() == ''
        runnable.getContainer().getVolumesList() == ['/mnt/foo/scratch:/mnt/foo/scratch:rw']
        and:
        instancePolicy.getDisksCount() == 0
        instancePolicy.getMachineType() == ''
        and:
        req.getAllocationPolicy().getNetwork().getNetworkInterfacesCount() == 0
        and:
        req.getLogsPolicy().getDestination().toString() == 'CLOUD_LOGGING'
    }

    def 'should create submit request/2' () {
        given:
        def WORK_DIR = CloudStorageFileSystem.forBucket('foo').getPath('/scratch')
        and:
        def CONTAINER_IMAGE = 'ubuntu:22.1'
        def CONTAINER_OPTS = '--this --that'
        def CPUS = 4
        def DISK = MemoryUnit.of('50 GB')
        def MACHINE_TYPE = 'vm-type-2'
        def MEM = MemoryUnit.of('8 GB')
        def TIMEOUT = Duration.of('1 hour')
        and:
        def exec = Mock(GoogleBatchExecutor) {
            getConfig() >> Mock(BatchConfig) {
                getSpot() >> true
                getNetwork() >> 'net-1'
                getSubnetwork() >> 'subnet-1'
                getUsePrivateAddress() >> true
            }
        }
        and:
        def bean = new TaskBean(workDir: WORK_DIR, inputFiles: [:])
        def task = Mock(TaskRun) {
            toTaskBean() >> bean
            getHashLog() >> 'abcd1234'
            getWorkDir() >> WORK_DIR
            getContainer() >> CONTAINER_IMAGE
            getConfig() >> Mock(TaskConfig) {
                getContainerOptions() >> CONTAINER_OPTS
                getCpus() >> CPUS
                getDisk() >> DISK
                getMachineType() >> MACHINE_TYPE
                getMemory() >> MEM
                getTime() >> TIMEOUT
            }
        }

        and:
        def handler = new GoogleBatchTaskHandler(task, exec)

        when:
        def req = handler.newSubmitRequest(task)
        then:
        def taskGroup = req.getTaskGroups(0)
        def runnable = taskGroup.getTaskSpec().getRunnables(0)
        def instancePolicy = req.getAllocationPolicy().getInstances(0).getPolicy()
        def networkInterface = req.getAllocationPolicy().getNetwork().getNetworkInterfaces(0)
        and:
        taskGroup.getTaskSpec().getComputeResource().getCpuMilli() == CPUS * 1_000
        taskGroup.getTaskSpec().getComputeResource().getMemoryMib() == MEM.toMega()
        taskGroup.getTaskSpec().getMaxRunDuration().getSeconds() == TIMEOUT.seconds
        and:
        runnable.getContainer().getCommandsList().join(' ') == '/bin/bash -o pipefail -c trap "{ cp .command.log /mnt/foo/scratch/.command.log; }" ERR; /bin/bash /mnt/foo/scratch/.command.run 2>&1 | tee .command.log'
        runnable.getContainer().getImageUri() == CONTAINER_IMAGE
        runnable.getContainer().getOptions() == CONTAINER_OPTS
        runnable.getContainer().getVolumesList() == ['/mnt/foo/scratch:/mnt/foo/scratch:rw']
        and:
        instancePolicy.getDisks(0).getNewDisk().getSizeGb() == DISK.toGiga()
        instancePolicy.getProvisioningModel().toString() == 'SPOT'
        instancePolicy.getMachineType() == MACHINE_TYPE
        and:
        networkInterface.getNetwork() == 'net-1'
        networkInterface.getSubnetwork() == 'subnet-1'
        networkInterface.getNoExternalIpAddress() == true
        and:
        req.getLogsPolicy().getDestination().toString() == 'CLOUD_LOGGING'
    }
}