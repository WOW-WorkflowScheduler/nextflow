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
import nextflow.k8s.K8sConfig

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
    private final String dns
    private boolean registered = false
    private boolean closed = false


    K8sSchedulerClient(K8sConfig.K8sScheduler schedulerConfig, String namespace, String runName) {
        this.schedulerConfig = schedulerConfig
        this.dns = schedulerConfig.getDNS()
        this.namespace = namespace ?: 'default'
        this.runName = runName
    }

    synchronized void registerScheduler() {
        if ( registered ) return
        registered = true;
        String url = "$dns/scheduler/registerScheduler/$namespace/$runName/${schedulerConfig.getStrategy()}"
        if( !url.startsWith( 'http://' ) && !url.startsWith( 'https://' )){
            throw new IllegalArgumentException( "Config: k8s.scheduler.dns ('${schedulerConfig.getDNS()}') does not start with http[s]://"  )
        }
        HttpURLConnection post = new URL(url).openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        post.setDoOutput(true)
        post.setRequestProperty("Content-Type", "application/json")
        String message = JsonOutput.toJson( [:] )
        try{
            post.getOutputStream().write(message.getBytes("UTF-8"))
        } catch ( UnknownHostException e ){
            throw new IllegalArgumentException( "The scheduler was not found under '$url', is the url correct and the scheduler running?" )
        } catch ( IOException e ){
            throw new IllegalStateException( "Cannot register scheduler under $url, got ${e.class.toString()}: ${e.getMessage()}", e )
        }
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from k8s scheduler while registering" )
        }
    }

    synchronized void closeScheduler(){
        if ( closed ) return
        closed = true;
        HttpURLConnection post = new URL("$dns/scheduler/$namespace/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "DELETE" )
        int responseCode = post.getResponseCode()
        log.trace "Delete scheduler code was: ${responseCode}"
    }

    Map registerTask( Map config ){

        HttpURLConnection post = new URL("$dns/scheduler/registerTask/$namespace/$runName").openConnection() as HttpURLConnection
        post.setRequestMethod( "POST" )
        String message = JsonOutput.toJson( config )
        post.setDoOutput(true)
        post.setRequestProperty("Content-Type", "application/json")
        post.getOutputStream().write(message.getBytes("UTF-8"));
        int responseCode = post.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while registering task: ${config.name}" )
        }
        Map response = new JsonSlurper().parse(post.getInputStream()) as Map
        return response

    }

    Map getTaskState( String podname ){

        HttpURLConnection get = new URL("$dns/scheduler/taskstate/$namespace/$runName/$podname").openConnection() as HttpURLConnection
        get.setRequestMethod( "GET" )
        get.setDoOutput(true)
        int responseCode = get.getResponseCode()
        if( responseCode != 200 ){
            throw new IllegalStateException( "Got code: ${responseCode} from nextflow scheduler, while requesting task state: $podname" )
        }
        Map response = new JsonSlurper().parse(get.getInputStream()) as Map
        return response

    }

}
