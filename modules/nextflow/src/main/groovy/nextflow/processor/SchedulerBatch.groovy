package nextflow.processor

interface SchedulerBatch {

    void startBatch()
    void endBatch()

}