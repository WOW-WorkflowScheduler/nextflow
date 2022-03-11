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

import javax.annotation.Nullable

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j
import nextflow.Const
import nextflow.exception.AbortOperationException
import nextflow.k8s.client.ClientConfig
import nextflow.k8s.client.K8sClient
import nextflow.k8s.client.K8sResponseException
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodNodeSelector
import nextflow.k8s.model.PodOptions
import nextflow.k8s.model.PodSecurityContext
import nextflow.k8s.model.PodVolumeClaim
import nextflow.k8s.model.ResourceType
import nextflow.util.Duration
import nextflow.processor.TaskRun
import java.nio.file.Path

/**
 * Model Kubernetes specific settings defined in the nextflow
 * configuration file
 * 
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class K8sConfig implements Map<String,Object> {

    @Delegate
    private Map<String,Object> target

    private PodOptions podOptions

    K8sConfig(Map<String,Object> config) {
        target = config ?: Collections.<String,Object>emptyMap()

        this.podOptions = createPodOptions(target.pod)
        if( getStorageClaimName() ) {
            final name = getStorageClaimName()
            final mount = getStorageMountPath()
            final subPath = getStorageSubPath()
            this.podOptions.volumeClaims.add(new PodVolumeClaim(name, mount, subPath))
        }

        if( getLocalPath() ) {
            final name = getLocalPath()
            final mount = getLocalStorageMountPath()
            this.podOptions.hostMount.add( new PodHostMount(name, mount) )
        }

        // -- shortcut to pod image pull-policy
        if( target.pullPolicy )
            podOptions.imagePullPolicy = target.pullPolicy.toString()
        else if( target.imagePullPolicy )
            podOptions.imagePullPolicy = target.imagePullPolicy.toString()

        // -- shortcut to pod security context
        if( target.runAsUser != null )
            podOptions.securityContext = new PodSecurityContext(target.runAsUser)
        else if( target.securityContext instanceof Map )
            podOptions.securityContext = new PodSecurityContext(target.securityContext as Map)
    }

    private PodOptions createPodOptions( value ) {
        if( value instanceof List )
            return new PodOptions( value as List )

        if( value instanceof Map )
            return new PodOptions( [(Map)value] )

        if( value == null )
            return new PodOptions()

        throw new IllegalArgumentException("Not a valid pod setting: $value")
    }

    Map<String,String> getLabels() {
        podOptions.getLabels()
    }

    Map<String,String> getAnnotations() {
        podOptions.getAnnotations()
    }

    K8sDebug getDebug() {
        new K8sDebug( (Map<String,Object>)get('debug') )
    }

    K8sScheduler getScheduler(){
        target.scheduler || locationAwareScheduling() ? new K8sScheduler( (Map<String,Object>)target.scheduler ) : null
    }

    Storage getStorage() {
        locationAwareScheduling() ? new Storage( (Map<String,Object>)target.storage, getLocalClaimPaths() ) : null
    }

    boolean getCleanup(boolean defValue=true) {
        target.cleanup == null ? defValue : Boolean.valueOf( target.cleanup as String )
    }

    String getUserName() {
        target.userName ?: System.properties.get('user.name')
    }

    String getStorageClaimName() {
        target.storageClaimName as String
    }

    String getLocalPath() {
        target.localPath as String
    }

    String getStorageMountPath() {
        target.storageMountPath ?: '/workspace' as String
    }

    String getLocalStorageMountPath() {
        target.localStorageMountPath ?: '/workspace' as String
    }

    String getStorageSubPath() {
        target.storageSubPath
    }

    /**
     * Whenever the pod should honour the entrypoint defined by the image (default: false)
     *
     *  @return  When {@code false} the launcher script is run by using pod `command` attributes which
     *      overrides the entrypoint point defined by the image.
     *
     *      When {@code true} the launcher is run via the pod `args` attribute, without altering the
     *      container entrypoint (it does however require to have a bash shell as the image entrypoint)
     *
     */
    boolean entrypointOverride() {
        def result = target.entrypointOverride
        if( result == null )
            result = System.getenv('NXF_CONTAINER_ENTRYPOINT_OVERRIDE')
        return result
    }

    /**
     * @return the path where the workflow is launched and the user data is stored
     */
    String getLaunchDir() {
        if( target.userDir ) {
            log.warn "K8s `userDir` has been deprecated -- Use `launchDir` instead"
            return target.userDir
        }
        target.launchDir ?: "${getStorageMountPath()}/${getUserName()}" as String
    }

    /**
     * @return Defines the path where the workflow temporary data is stored. This must be a path in a shared K8s persistent volume (default:<user-dir>/work).
     */
    String getWorkDir() {
        target.workDir ?: "${getLaunchDir()}/work" as String
    }

    /**
     * @return Defines the path where Nextflow projects are downloaded. This must be a path in a shared K8s persistent volume (default: <volume-claim-mount-path>/projects).
     */
    String getProjectDir() {
        target.projectDir ?: "${getStorageMountPath()}/projects" as String
    }

    String getNamespace() { target.namespace }

    boolean useJobResource() { ResourceType.Job.name() == target.computeResourceType?.toString() }

    String getServiceAccount() { target.serviceAccount }

    String getNextflowImageName() {
        final defImage = "nextflow/nextflow:${Const.APP_VER}"
        return target.navigate('nextflow.image', defImage)
    }

    boolean getAutoMountHostPaths() {
        Boolean.valueOf( target.autoMountHostPaths as String )
    }

    boolean locationAwareScheduling() {
        getLocalClaimPaths().size() > 0
    }

    PodOptions getPodOptions() {
        podOptions
    }

    @Memoized
    boolean fetchNodeName() {
        Boolean.valueOf( target.fetchNodeName as String )
    }

    /**
     * @return the collection of defined volume claim names
     */
    Collection<String> getClaimNames() {
        podOptions.volumeClaims.collect { it.claimName }
    }

    Collection<String> getClaimPaths() {
        podOptions.volumeClaims.collect { it.mountPath }
    }

    Collection<String> getLocalClaimPaths() {
        podOptions.hostMount.collect { it.mountPath }
    }

    /**
     * Find a volume claim name given the mount path
     *
     * @param path The volume claim mount path
     * @return The volume claim name for the given mount path
     */
    String findVolumeClaimByPath(String path) {
        def result = podOptions.volumeClaims.find { path.startsWith(it.mountPath) }
        return result ? result.claimName : null
    }

    String findLocalVolumeClaimByPath(String path) {
        def result = podOptions.hostMount.find { path.startsWith(it.mountPath) }
        return result ? result.hostPath : null
    }

    @Memoized
    ClientConfig getClient() {

        final result = ( target.client instanceof Map
                ? clientFromNextflow(target.client as Map, target.namespace as String, target.serviceAccount as String)
                : clientDiscovery(target.context as String, target.namespace as String, target.serviceAccount as String)
        )

        if( target.httpConnectTimeout )
            result.httpConnectTimeout = target.httpConnectTimeout as Duration

        if( target.httpReadTimeout )
            result.httpReadTimeout = target.httpReadTimeout as Duration

        if( target.maxErrorRetry )
            result.maxErrorRetry = target.maxErrorRetry as Integer

        return result
    }

    /**
     * Get the K8s client config from the declaration made in the Nextflow config file
     *
     * @param map
     *      A map representing the clint configuration options define in the nextflow
     *      config file
     * @param namespace
     *      The K8s namespace to be used. If omitted {@code default} is used.
     * @param serviceAccount
     *      The K8s service account to be used. If omitted {@code default} is used.
     * @return
     *      The Kubernetes {@link ClientConfig} object
     */
    @PackageScope ClientConfig clientFromNextflow(Map map, @Nullable String namespace, @Nullable String serviceAccount ) {
        ClientConfig.fromNextflowConfig(map,namespace,serviceAccount)
    }

    /**
     * Discover the K8s client config from the execution environment
     * that can be either a `.kube/config` file or service meta file
     * when running in a pod.
     *
     * @param contextName
     *      The name of the configuration context to be used
     * @param namespace
     *      The Kubernetes namespace to be used
     * @param serviceAccount
     *      The Kubernetes serviceAccount to be used
     * @return
     *      The discovered Kube {@link ClientConfig} object
     */
    @PackageScope ClientConfig clientDiscovery(String contextName, String namespace, String serviceAccount) {
        ClientConfig.discover(contextName, namespace, serviceAccount)
    }

    void checkStorageAndPaths(K8sClient client, String pipelineName) {
        if( !getStorageClaimName() )
            throw new AbortOperationException("Missing K8s storage volume claim -- The name of a persistence volume claim needs to be provided in the nextflow configuration file")

        log.debug "Kubernetes workDir=$workDir; projectDir=$projectDir; volumeClaims=${getClaimNames()}"

        for( String name : getClaimNames() ) {
            try {
                client.volumeClaimRead(name)
            }
            catch (K8sResponseException e) {
                if( e.response.code == 404 ) {
                    throw new AbortOperationException("Unknown volume claim: $name -- make sure a persistent volume claim with the specified name is defined in your K8s cluster")
                }
                else throw e
            }
        }

        if( !findVolumeClaimByPath(getLaunchDir()) )
            throw new AbortOperationException("Kubernetes `launchDir` must be a path mounted as a persistent volume -- launchDir=$launchDir; volumes=${getClaimPaths().join(', ')}")

        if( !findVolumeClaimByPath(getWorkDir()) )
            throw new AbortOperationException("Kubernetes `workDir` must be a path mounted as a persistent volume -- workDir=$workDir; volumes=${getClaimPaths().join(', ')}")

        if( !findVolumeClaimByPath(getProjectDir()) )
            throw new AbortOperationException("Kubernetes `projectDir` must be a path mounted as a persistent volume -- projectDir=$projectDir; volumes=${getClaimPaths().join(', ')}")

        //The nextflow project/workflow has to be on a shared drive
        if ( pipelineName && pipelineName[0] == '/' && !findVolumeClaimByPath(pipelineName) )
            throw new AbortOperationException("Kubernetes `pipelineName` must be a path mounted as a persistent volume -- projectDir=$pipelineName; volumes=${getClaimPaths().join(', ')}")

        if( getStorage() && !findLocalVolumeClaimByPath(getStorage().getWorkdir()) )
            throw new AbortOperationException("Kubernetes `storage.workdir` must be a path mounted as a local volume -- storage.workdir=${getStorage().getWorkdir()}; volumes=${getLocalClaimPaths().join(', ')}")

    }

    @CompileStatic
    static class K8sDebug {

        @Delegate
        Map<String,Object> target

        K8sDebug(Map<String,Object> debug) {
            this.target = debug ?: Collections.<String,Object>emptyMap()
        }

        boolean getYaml() { Boolean.valueOf( target.yaml as String ) }
    }

    @CompileStatic
    static class K8sScheduler {

        @Delegate
        Map<String,Object> target

        K8sScheduler(Map<String,Object> scheduler) {
            this.target = scheduler
        }

        String getName() { target.name as String ?: 'workflow-scheduler' }

        String getStrategy() { target.strategy as String ?: 'FIFO' }

        String getServiceAccount() { target.serviceAccount as String }

        String getImagePullPolicy() { target.imagePullPolicy as String }

        Integer getCPUs() { target.cpu as Integer ?: 1 }

        String getContainer() { target.container as String }

        String getCommand() { target.command as String }

        Integer getPort() { target.port as Integer ?: 8080 }

        String getWorkDir() { target.workDir as String }

    }

    @CompileStatic
    static class Storage {


        @Delegate
        Map<String,Object> target
        Collection<String> localClaims

        Storage(Map<String,Object> scheduler, Collection<String> localClaims ) {
            this.target = scheduler
            this.localClaims = localClaims
        }

        String getCopyStrategy() {
            target.copyStrategy as String ?: 'ftp'
        }

        String getWorkdir() {
            Path workdir = (target.workdir ?: localClaims[0]) as Path
            if( ! workdir.getName().equalsIgnoreCase('localWork') ){
                workdir = workdir.resolve( 'localWork' )
            }
            return workdir.toString()
        }

        PodNodeSelector getNodeSelector(){
            return target.nodeSelector ? new PodNodeSelector( target.nodeSelector ) : null
        }

        boolean deleteIntermediateData(){
            target.deleteIntermediateData as Boolean ?: false
        }

        String getImageName() {
            target.imageName ?: 'alpine/k8s:1.20.7'
        }

        String getCmd() {
            target.cmd as String ?: "./$TaskRun.CMD_INIT_RUN"
        }

    }
}

