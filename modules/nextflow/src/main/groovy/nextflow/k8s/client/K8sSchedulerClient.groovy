/*
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

package nextflow.k8s.client

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import nextflow.file.LocalFileWalker
import nextflow.k8s.K8sConfig
import nextflow.k8s.localdata.LocalPath
import nextflow.k8s.model.PodHostMount
import nextflow.k8s.model.PodSpecBuilder
import nextflow.k8s.model.PodVolumeClaim

import java.nio.file.Path
import java.nio.file.Paths

/**
 * K8sScheduler API client
 *
 * @author Fabian Lehmann <fabian.lehmann@informatik.hu-berlin.de>
 */
@Slf4j
class K8sSchedulerClient {

    private final K8sConfig.K8sScheduler schedulerConfig
    private final String namespace
    private final String runName
    private boolean registered = false
    private boolean closed = false
    private final K8sClient k8sClient
    private final Collection<PodHostMount> hostMounts
    private final Collection<PodVolumeClaim> volumeClaims
    private String ip
    private int tasksInBatch = 0;


    K8sSchedulerClient(K8sConfig.K8sScheduler schedulerConfig, String namespace, String runName, K8sClient k8sClient,
                       Collection<PodHostMount> hostMounts, Collection<PodVolumeClaim> volumeClaims) {
        this.volumeClaims = volumeClaims
        this.hostMounts = hostMounts
        this.k8sClient = k8sClient
        this.schedulerConfig = schedulerConfig
        this.namespace = namespace ?: 'default'
        this.runName = runName
        LocalFileWalker.createLocalPath = (Path path, LocalFileWalker.FileAttributes attr, Path workDir) -> LocalPath.toLocalPath( path, this, attr, workDir )
    }

    private String getDNS(){
        return "http://${ip.replace('.','-')}.${namespace}.pod.cluster.local:${schedulerConfig.getPort()}"
    }

    private void startScheduler(){

        boolean start = false
        Map state

        try{
            //If no pod with the name exists an exceptions is thrown
            state = k8sClient.podState( schedulerConfig.getName() )
            if( state.terminated ) {
                k8sClient.podDelete( schedulerConfig.getName() )
                start = true
                log.info "Scheduler ${schedulerConfig.getName()} is terminated"
            } else if( state.running || state.waiting ) log.trace "Scheduler ${schedulerConfig.getName()} is already running"
            else log.error "Unknown state for ${schedulerConfig.getName()}: ${state.toString()}"

        } catch ( K8sResponseException e ) {
            if ( e.getErrorCode() == 404 ) start = true
            else log.error( "Got unexpected HTTP code ${e.getErrorCode()} while checking scheduler's state", e.message )
        }

        if( start ){
            log.trace "Scheduler ${schedulerConfig.getName()} is not running, let's start"
            final builder = new PodSpecBuilder()
                    .withImageName( schedulerConfig.getContainer() )
                    .withPodName( schedulerConfig.getName() )
                    .withCpus( schedulerConfig.getCPUs() )
                    .withImagePullPolicy( schedulerConfig.getImagePullPolicy() )
                    .withServiceAccount( schedulerConfig.getServiceAccount() )
                    .withNamespace( namespace )
                    .withLabel('component', 'scheduler')
                    .withLabel('tier', 'control-plane')
                    .withHostMounts( hostMounts )
                    .withVolumeClaims( volumeClaims )
                    .withWorkDir( schedulerConfig.getWorkDir() )

            if( schedulerConfig.getCommand() )
                builder.withCommand( schedulerConfig.getCommand() )

            Map pod = builder.build()

            List env = [[
                    name: 'SCHEDULER_NAME',
                    value: schedulerConfig.getName()
            ]]

            Map container = pod.spec.containers.get(0) as Map
            container.put('env', env)

            k8sClient.podCreate( pod, Paths.get('.nextflow-scheduler.yaml'), namespace)
        }

        //wait for scheduler to get ready
        def i = 0
        do {
            sleep(100)
            state = k8sClient.podState( schedulerConfig.getName() )
            //log state every 2 seconds
            if( i++ % 20 ) log.trace "Waiting for scheduler to start, current state: ${state.toString()}"
        } while ( state.waiting );

        ip = k8sClient.podIP( schedulerConfig.getName() )
        if( !state.running ) throw new IllegalStateException( "Scheduler pod ${schedulerConfig.getName()} was not started, state: ${state.toString()}" )

    }

    synchronized void registerScheduler( Map data ) {
        if ( registered ) return

        startScheduler()

        String url = "${getDNS()}/scheduler/registerScheduler/$namespace/$runName/${schedulerConfig.getStrategy()}"
        registered = true;
        int trials = 0
        while ( trials++ < 50 ) {
            try {
                HttpURLConnection post = new URL(url).openConnection() as HttpURLConnection
                post.setRequestMethod( "POST" )
                post.setDoOutput(true)
                post.setRequestProperty("Content-Type", "application/json")
                data.dns = getDNS()
                String message = JsonOutput.toJson( data )
                post.getOutputStream().write(message.getBytes("UTF-8"))
                int responseCode = post.getResponseCode()
                if( responseCode != 200 ){
                    throw new IllegalStateException( "Got code: ${responseCode} from k8s scheduler while registering" )
                }
                return
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("The scheduler was not found under '$url', is the url correct and the scheduler running?")
            } catch (ConnectException e) {
                Thread.sleep( 3000 )
            }catch (IOException e) {
                throw new IllegalStateException("Cannot register scheduler under $url, got ${e.class.toString()}: ${e.getMessage()}", e)
            }
        }
        throw new IllegalStateException("Cannot connect to scheduler under $url" )
    }

    synchronized void closeScheduler(){
        if ( closed ) return
        closed = true;
        HttpURLConnection post = new URL("${getDNS()}/scheduler/$namespace/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "DELETE" )
        int responseCode = post.getResponseCode()
        log.trace "Delete scheduler code was: ${responseCode}"
    }

    Map registerTask( Map config ){

        HttpURLConnection post = new URL("${getDNS()}/scheduler/registerTask/$namespace/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( config )
        post.setDoOutput(true)
        post.setRequestProperty("Content-Type", "application/json")
        post.getOutputStream().write(message.getBytes("UTF-8"));
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while registering task: ${config.name}" )
        }
        tasksInBatch++
        Map response = new JsonSlurper().parse(post.getInputStream()) as Map
        return response

    }

    private void batch( String command ){
        HttpURLConnection post = new URL("${getDNS()}/scheduler/${command}Batch/$namespace/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        if ( command == 'end' ){
            post.setDoOutput(true)
            post.setRequestProperty("Content-Type", "application/json")
            post.getOutputStream().write("$tasksInBatch".getBytes("UTF-8"));
        }
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while ${command}ing batch" )
        }
    }

    void startBatch(){
        tasksInBatch = 0
        batch('start')
    }

    void endBatch(){
        batch('end')
    }

    Map getTaskState( String podname ){

        HttpURLConnection get = new URL("${getDNS()}/scheduler/taskstate/$namespace/$runName/$podname").openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting task state: $podname" )
        }
        Map response = new JsonSlurper().parse(get.getInputStream()) as Map
        return response

    }

    Map getFileLocation( String path ){

        HttpURLConnection get = new URL("${getDNS()}/file/$namespace/$runName").openConnection() as HttpURLConnection
        get.setRequestMethod( "POST" )
        get.setDoOutput(true)
        get.setRequestProperty("Content-Type", "application/json")
        get.getOutputStream().write("$path".getBytes("UTF-8"));
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting file location: $path (${get.responseMessage})" )
        }
        Map response = new JsonSlurper().parse(get.getInputStream()) as Map
        return response

    }

    String getDaemonOnNode( String node ){

        HttpURLConnection get = new URL("${getDNS()}/daemon/$namespace/$runName/$node").openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting daemon on node: $node" )
        }
        String response = new JsonSlurper().parse(get.getInputStream()) as String
        return response

    }

    void addFileLocation( String path, long size, long timestamp, boolean overwrite, String node = null ){

        String method = overwrite ? 'overwrite' : 'add'

        HttpURLConnection get = new URL("${getDNS()}/file/location/${method}/$namespace/$runName${ node ? "/$node" : ''}").openConnection() as HttpURLConnection
        get.setRequestMethod( "POST" )
        get.setDoOutput(true)
        Map data = [
            path      : path,
            size      : size,
            timestamp : timestamp
        ]
        if ( node ){
            data.node = node
        }
        String message = JsonOutput.toJson( data )
        get.setRequestProperty("Content-Type", "application/json")
        get.getOutputStream().write(message.getBytes("UTF-8"));
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while updating file location: $path: $node (${get.responseMessage})" )
        }

    }

}
