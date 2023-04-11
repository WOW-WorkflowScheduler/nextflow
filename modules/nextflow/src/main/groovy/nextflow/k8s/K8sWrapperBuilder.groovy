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
        this(task)
        this.storage = storage
        if( storage ){
            switch (storage.getCopyStrategy().toLowerCase()) {
                case 'copy':
                case 'ftp':
                    if ( this.scratch == null || this.scratch == true ){
                        //Reduce amount of local data
                        this.scratch = (storage.getWorkdir() as Path).resolve( "scratch" ).toString()
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

    private String getStorageLocalWorkDir() {
        String localWorkDir = storage.getWorkdir()
        if ( !localWorkDir.endsWith("/") ){
            localWorkDir += "/"
        }
        localWorkDir
    }

    @Override
    protected Map<String, String> makeBinding() {
        final Map<String,String> binding = super.makeBinding()
        if ( binding.stage_inputs && storage && localWorkDir && !storage.withInitContainers() && !storage.separateCopy() ) {
            final String cmd = """\
                    # copy inputs from other nodes
                    local s="\$PWD"
                    cd "${workDir.toString()}"
                    ${storage.getCmd().strip()} ${isTraceRequired()} &> ${TaskRun.CMD_INIT_LOG}
                    status=\$?
                    if [ ! \$status -eq 0 ]; then
                        echo "Error: ${storage.getCmd()} failed with status \$status"
                        touch .command.init.failure
                        exit 123
                    fi
                    cd "\$s" 
            """.stripIndent()
            binding.stage_inputs = cmd + binding.stage_inputs
        }
        if ( binding.stage_inputs && storage && localWorkDir && storage.separateCopy() ) {
            final String cmd = """\
                    # create symlinks
                    if test -f "${workDir.toString()}/.command.symlinks"; then
                        bash "${workDir.toString()}/.command.symlinks" || true
                    fi 
            """.stripIndent()
            binding.stage_inputs = cmd + binding.stage_inputs
        }
        return binding
    }

    @Override
    protected String getLaunchCommand(String interpreter, String env) {
        String cmd = ''
        if( storage && localWorkDir ){
            cmd += "local INFILESTIME=\$(/etc/nextflow/getStatsAndResolveSymlinks infiles \"${workDir.toString()}/.command.infiles\" \"${getStorageLocalWorkDir()}\" \"\$PWD/\" || true)\n"
        }
        cmd += super.getLaunchCommand(interpreter, env)
        if( storage && localWorkDir && isTraceRequired() ){
            cmd += "\nlocal exitCode=\$?"
            cmd += """\necho \"infiles_time=\${INFILESTIME}" >> ${TaskRun.CMD_TRACE}\n"""
            cmd += "return \$exitCode\n"
        }
        return cmd
    }

    @Override
    String getCleanupCmd(String scratch) {
        String cmd = super.getCleanupCmd( scratch )
        if( storage && localWorkDir ){
            cmd += "mkdir -p \"${localWorkDir.toString()}/\" || true\n"
            cmd += "local OUTFILESTIME=\$(/etc/nextflow/getStatsAndResolveSymlinks outfiles \"${workDir.toString()}/.command.outfiles\" \"${getStorageLocalWorkDir()}\" \"${localWorkDir.toString()}/\" || true)\n"
            if ( isTraceRequired() ) {
                cmd += "echo \"outfiles_time=\${OUTFILESTIME}\" >> ${workDir.resolve(TaskRun.CMD_TRACE)}"
            }
        }
        return cmd
    }

}
