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

import com.google.common.hash.Hashing
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.dag.DAG
import nextflow.executor.Executor
import nextflow.fusion.FusionHelper
import nextflow.file.FileHelper
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.client.K8sSchedulerClient
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodMountConfig
import nextflow.k8s.model.PodOptions
import nextflow.k8s.model.PodVolumeClaim
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.ConfigHelper
import nextflow.util.Duration
import nextflow.util.ServiceName

import java.nio.file.NoSuchFileException
import java.nio.file.Path
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

    private static K8sSchedulerClient schedulerClient

    static getK8sSchedulerClient(){
        schedulerClient
    }

    /**
     * Name of the created daemonSet
     */
    private String daemonSet = null

    private K8sSchedulerBatch schedulerBatch = null

    protected K8sClient getClient() {
        client
    }

    @PackageScope K8sSchedulerClient getSchedulerClient() {
        schedulerClient
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
            registerGetStatsConfigMap()
        }

        final K8sConfig.K8sScheduler schedulerConfig = k8sConfig.getScheduler()

        if( schedulerConfig ) {
            schedulerClient = new K8sSchedulerClient(schedulerConfig, k8sConfig.getNamespace(), session.runName, client,
                    k8sConfig.getPodOptions().getHostMount(), k8sConfig.getPodOptions().getVolumeClaims())
            this.schedulerBatch?.setSchedulerClient( schedulerClient )
            final PodOptions podOptions = k8sConfig.getPodOptions()
            Boolean traceEnabled = session.config.navigate('trace.enabled') as Boolean
            Map data = [
                    workDir : k8sConfig.getStorage()?.getWorkdir(),
                    localClaims : podOptions.hostMount,
                    volumeClaims : podOptions.volumeClaims,
                    copyStrategy : k8sConfig.getStorage()?.getCopyStrategy(),
                    locationAware : k8sConfig.locationAwareScheduling(),
                    traceEnabled : traceEnabled,
                    costFunction : schedulerConfig.getCostFunction(),
                    maxCopyTasksPerNode : schedulerConfig.getMaxCopyTasksPerNode(),
                    maxWaitingCopyTasksPerNode : schedulerConfig.getMaxWaitingCopyTasksPerNode()
            ]

            schedulerClient.registerScheduler( data )
        }

    }

    protected void registerGetStatsConfigMap() {
        Map<String,String> configMap = [:]

        final statFile = '/usr/local/bin/getStatsAndResolveSymlinks' as Path
        final content = statFile.bytes.encodeBase64().toString()
        configMap['getStatsAndResolveSymlinks'] = content

        String configMapName = makeConfigMapName(content)
        tryCreateConfigMap(configMapName, configMap)
        log.debug "Created K8s configMap with name: $configMapName"
        k8sConfig.getPodOptions().getMountConfigMaps().add( new PodMountConfig(configMapName, '/etc/nextflow', 0111) )
    }

    protected void tryCreateConfigMap(String name, Map<String,String> data) {
        try {
            client.configCreateBinary(name, data)
        }
        catch( K8sResponseException e ) {
            if( e.response.reason != 'AlreadyExists' )
                throw e
        }
    }

    protected String makeConfigMapName( String content ) {
        "nf-get-stat-${hash(content)}"
    }

    protected String hash( String text) {
        def hasher = Hashing .murmur3_32() .newHasher()
        hasher.putUnencodedChars(text)
        return hasher.hash().toString()
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
            volumes << [name: name, hostPath: [path: entry.hostPath]]
        }

        final namesMap = [:]

        // creates a volume name for each unique claim name
        for( String claimName : podOptions.volumeClaims.collect { it.claimName }.unique() ) {
            final volName = 'vol-' + volume++
            namesMap[claimName] = volName
            volumes << [name: volName, persistentVolumeClaim: [claimName: claimName]]
        }

        // -- volume claims
        for( PodVolumeClaim entry : podOptions.volumeClaims ) {
            //check if we already have a volume for the pvc
            final name = namesMap.get(entry.claimName)
            final claim = [name: name, mountPath: entry.mountPath ]
            if( entry.subPath )
                claim.subPath = entry.subPath
            if( entry.readOnly )
                claim.readOnly = entry.readOnly
            mounts << claim
        }

        String name = "mount-${session.runName.replace('_', '-')}"
        def spec = [
                containers: [ [
                                      name: name,
                                      image: k8sConfig.getStorage().getImageName(),
                                      volumeMounts: mounts,
                                      imagePullPolicy : 'IfNotPresent'
                              ] ],
                volumes: volumes,
                serviceAccount: client.config.serviceAccount
        ]

        if( k8sConfig.getStorage().getNodeSelector() )
            spec.put( 'nodeSelector', k8sConfig.getStorage().getNodeSelector().toSpec() as Serializable )

        def pod = [
                apiVersion: 'apps/v1',
                kind: 'DaemonSet',
                metadata: [
                        labels: [
                                app: 'nextflow'
                        ],
                        name: name,
                        namespace: k8sConfig.getNamespace() ?: 'default'
                ],
                spec : [
                        restartPolicy: 'Always',
                        template: [
                                metadata: [
                                        labels: [
                                                name : name,
                                                app: 'nextflow'
                                        ]
                                ],
                                spec: spec,
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

    @Override
    void shutdown() {
        final K8sConfig.K8sScheduler schedulerConfig = k8sConfig.getScheduler()
        if( schedulerConfig ) {
            try{
                schedulerClient.closeScheduler()
            } catch (Exception e){
                log.error( "Error while closing scheduler", e)
            }
        }

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
        if ( k8sConfig.getScheduler()?.getBatchSize() > 1 ) {
            this.schedulerBatch = new K8sSchedulerBatch( k8sConfig.getScheduler().getBatchSize() )
        }
        TaskPollingMonitor.create(session, name, 100, Duration.of('5 sec'), this.schedulerBatch )
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
    
    void informDagChange( List<DAG.Vertex> processedVertices ) {
        schedulerClient?.informDagChange( processedVertices )
    }

}
