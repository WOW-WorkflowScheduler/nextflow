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
import groovy.util.logging.Slf4j
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskRun
import nextflow.util.Escape

/**
 * Implements a BASH wrapper for tasks executed by kubernetes cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@Slf4j
class K8sWrapperBuilder extends BashWrapperBuilder {

    boolean fileOutput

    K8sWrapperBuilder(TaskRun task, boolean fileOutput) {
        super(task)
        this.fileOutput = fileOutput
    }

    K8sWrapperBuilder(TaskRun task) {
        super(task)
        this.headerScript = "NXF_CHDIR=${Escape.path(task.workDir)}"
    }

    /**
     * only for testing purpose -- do not use
     */
    protected K8sWrapperBuilder() {}


    @Override
    String getCleanupCmd(String scratch) {
        String cmd = super.getCleanupCmd( scratch )
        if( fileOutput ){
            cmd += "find -L ${workDir.toString()} -exec stat --format \"%N;%b;%F;%x;%y;%z\" {} \\;"
            cmd += "> ${workDir.toString()}/.command.outfiles"
        }
        return cmd
    }

}
