package nextflow.k8s;

import nextflow.k8s.client.K8sSchedulerClient;
import nextflow.processor.SchedulerBatch;

class K8sSchedulerBatch implements SchedulerBatch {

    private K8sSchedulerClient schedulerClient

    void setSchedulerClient(K8sSchedulerClient schedulerClient) {
        this.schedulerClient = schedulerClient
    }

    @Override
    void startBatch() {
        schedulerClient.startBatch()
    }

    @Override
    void endBatch() {
        schedulerClient.endBatch()
    }
}
