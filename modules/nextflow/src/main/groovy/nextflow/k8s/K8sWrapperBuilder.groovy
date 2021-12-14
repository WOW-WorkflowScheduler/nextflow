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
import nextflow.file.FileHelper
import nextflow.processor.TaskRun
import nextflow.util.Escape
import java.nio.file.Path

/**
 * Implements a BASH wrapper for tasks executed by kubernetes cluster
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
@Slf4j
class K8sWrapperBuilder extends BashWrapperBuilder {

    K8sConfig.Storage storage

    K8sWrapperBuilder(TaskRun task, K8sConfig.Storage storage) {
        super(task)
        this.storage = storage
        if( storage ){
            switch (storage.getCopyStrategy().toLowerCase()) {
                case 'copy':
                case 'ftp':
                    if ( !this.scratch ){
                        //Reduce amount of local data
                        this.scratch = true
                        this.stageOutMode = 'move'
                    }
                    break
            }
            if ( !this.targetDir || workDir == targetDir ) {
                this.localWorkDir = FileHelper.getWorkFolder(storage.getWorkdir() as Path, this.getHash())
            }
        }
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
    protected Map<String,String> makeBinding() {
        def binding = super.makeBinding()
        binding.K8sResolveSymlinks = """\
            getStatsAndResolveSymlinks() {
                local STARTFILE="\$1"
                local ENDFILE="\$(readlink -f "\$STARTFILE")"
                [ -f "\$ENDFILE" ] && local EXISTS=1 || local EXISTS=0
                local INFO="\$(stat -c "%s;%F;%w;%x;%y" "\$ENDFILE")"
                [ "\$STARTFILE" = "\$ENDFILE" ] && local ENDFILE=""
                local OUTPUT="\$STARTFILE;\$EXISTS;\$ENDFILE;\$INFO"
                echo "\$OUTPUT"
            }
            export -f getStatsAndResolveSymlinks
            """.stripIndent(true)
        return binding
    }

    @Override
    protected String getLaunchCommand(String interpreter, String env) {
        String cmd = ''
        if( storage && localWorkDir ){
            cmd += "find -L \$PWD -exec bash -c \"getStatsAndResolveSymlinks '{}'\" \\;"
            cmd += "> ${workDir.toString()}/.command.infiles || true\n"
        }
        cmd += super.getLaunchCommand(interpreter, env)
        return cmd
    }

    @Override
    String getCleanupCmd(String scratch) {
        String cmd = super.getCleanupCmd( scratch )
        if( storage && localWorkDir ){
            cmd += "find -L ${localWorkDir.toString()} -exec bash -c \"getStatsAndResolveSymlinks '{}'\" \\;"
            cmd += "> ${workDir.toString()}/.command.outfiles || true"
        }
        return cmd
    }

}
