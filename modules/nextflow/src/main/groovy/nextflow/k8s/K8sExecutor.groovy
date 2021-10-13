/*
 * Copyright 2013-2023, Seqera Labs
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
 */

package nextflow.k8s

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.fusion.FusionHelper
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodOptions
import nextflow.k8s.model.PodVolumeClaim
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName

import java.nio.file.Paths

/**
 * Implement the Kubernetes executor
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@ServiceName('k8s')
class K8sExecutor extends Executor {

    /**
     * The Kubernetes HTTP client
     */
    private K8sClient client

    /**
     * Name of the created daemonSet
     */
    private String daemonSet = null;

    protected K8sClient getClient() {
        client
    }

    /**
     * @return The `k8s` configuration scope in the nextflow configuration object
     */
    @Memoized
    protected K8sConfig getK8sConfig() {
        new K8sConfig( (Map<String,Object>)session.config.k8s )
    }

    /**
     * Initialise the executor setting-up the kubernetes client configuration
     */
    @Override
    protected void register() {
        super.register()
        final k8sConfig = getK8sConfig()
        final clientConfig = k8sConfig.getClient()
        this.client = new K8sClient(clientConfig)
        log.debug "[K8s] config=$k8sConfig; API client config=$clientConfig"

        //Create Daemonset to access local path on every node, maybe there is a better point to do this
        if( k8sConfig.locationAwareScheduling() ) {
            createDaemonSet()
        }
        
    }

    private void createDaemonSet(){

        final K8sConfig k8sConfig = getK8sConfig()
        final PodOptions podOptions = k8sConfig.getPodOptions()
        final mounts = []
        final volumes = []
        int volume = 1

        // host mounts
        for( PodHostMount entry : podOptions.hostMount ) {
            final name = 'vol-' + volume++
            mounts << [name: name, mountPath: entry.mountPath]
            volumes << [name: name, hostPath: [path: entry.hostPath], type: 'DirectoryOrCreate']
        }

        String name = "mount-${session.runName.replace('_', '-')}"
        def pod = [
                apiVersion: 'apps/v1',
                kind: 'DaemonSet',
                metadata: [
                        name: name,
                        namespace: k8sConfig.getNamespace() ?: 'default'
                ],
                spec : [
                        restartPolicy: 'Always',
                        template: [
                                metadata: [
                                        labels: [
                                                name : name
                                        ]
                                ],
                                spec: [
                                        containers: [ [
                                                              name: name,
                                                              image: k8sConfig.daemonSet(),
                                                              command: ['/bin/sh','-c', 'sleep infinity'],
                                                              volumeMounts: mounts
                                                      ] ],
                                        volumes: volumes,
                                        serviceAccount: client.config.serviceAccount
                                ],
                        ],
                        selector: [
                                matchLabels: [
                                        name: name
                                ]
                        ]
                ]
        ]

        daemonSet = name
        client.daemonSetCreate(pod, Paths.get('.nextflow-daemonset.yaml') )
        log.trace "Created daemonSet: $name"

    }

    void close(){
        if( daemonSet ){
            try {
                def result = client.daemonSetDelete( daemonSet )
                log.trace "$result"
            } catch (K8sResponseException e){
                log.error("Couldn't delete daemonset: $daemonSet", e)
            }
        }
        log.trace "Close K8s Executor"
    }


    /**
     * @return {@code true} since containerised execution is managed by Kubernetes
     */
    boolean isContainerNative() {
        return true
    }

    @Override
    String containerConfigEngine() {
        return 'docker'
    }

    /**
     * @return A {@link TaskMonitor} associated to this executor type
     */
    @Override
    protected TaskMonitor createTaskMonitor() {
        TaskPollingMonitor.create(session, name, 100, Duration.of('5 sec'))
    }

    /**
     * Creates a {@link TaskHandler} for the given {@link TaskRun} instance
     *
     * @param task A {@link TaskRun} instance representing a process task to be executed
     * @return A {@link K8sTaskHandler} instance modeling the execution in the K8s cluster
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.trace "[K8s] launching process > ${task.name} -- work folder: ${task.workDirStr}"
        new K8sTaskHandler(task,this)
    }

    @Override
    boolean isFusionEnabled() {
        return FusionHelper.isFusionEnabled(session)
    }
}
