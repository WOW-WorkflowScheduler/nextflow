package nextflow.file

import groovy.util.logging.Slf4j

import java.nio.file.FileVisitOption
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

@Slf4j
class LocalFileWalker {

    public static Path walkFileTree(Path start,
                                    Set<FileVisitOption> options,
                                    int maxDepth,
                                    FileVisitor<? super Path> visitor)
    {

        String parent = null
        boolean skip = false

        File file = new File( start.toString() + File.separatorChar + ".command.outfiles" )
        String line;
        file.withReader { reader ->
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(';')
                int i = 0
                String path = data[i++]
                Long bytes = data[i++] as Long
                String fileType = data[i++]
                String accessDate = data[i++]
                String modificationDate = data[i++]
                String changeDate = data[i++]

                if( path.startsWith('\'') && path.endsWith('\'') ) {
                    path = path.substring( 1, path.length() - 1 )
                }

                //If Symlink & not followLinks
                //split path
                //save it, and ignore everything at the path behind that

                if ( parent && path.startsWith(parent) ) {
                    if ( skip ) {
                        log.info "Skip $path"
                        continue
                    }
                } else {
                    skip = false
                }
                Boolean link = false

                if (fileType == 'symbolic link') {
                    path = path.split("\' -> \'")[1]
                    Path p = Paths.get(path)
                    while ( Files.isSymbolicLink(p) ) {
                        p = Files.readSymbolicLink(p)
                    }
                    if ( Files.isRegularFile(p) ) {
                        fileType = 'file'
                    } else if ( Files.isDirectory(p) ) {
                        fileType = 'directory'
                    }
                }
                if (fileType == 'directory') {
                    FileAttributes attributes = new FileAttributes( true , link, bytes )
                    def visitDirectory = visitor.preVisitDirectory(Paths.get(path), attributes);
                    if( visitDirectory == FileVisitResult.SKIP_SUBTREE ){
                        skip = true
                        parent = path
                    }
                } else if ( fileType.contains( 'file' ) ) {
                    FileAttributes attributes = new FileAttributes( false, link, bytes )
                    visitor.visitFile(Paths.get(path), attributes)
                } else {
                    log.error( "Unknown type: $fileType" )
                }
            }
        }

        return start
    }

    static class FileAttributes implements BasicFileAttributes {

        private final boolean directory
        private final boolean link
        private final long size

        FileAttributes( Boolean directory, Boolean link, Long size) {
            this.directory = directory
            this.link = link
            this.size = size
        }

        @Override
        FileTime lastModifiedTime() {
            return null
        }

        @Override
        FileTime lastAccessTime() {
            return null
        }

        @Override
        FileTime creationTime() {
            return null
        }

        @Override
        boolean isRegularFile() {
            return true
        }

        @Override
        boolean isDirectory() {
            return directory
        }

        @Override
        boolean isSymbolicLink() {
            return link
        }

        @Override
        boolean isOther() {
            return false
        }

        @Override
        long size() {
            return size
        }

        @Override
        Object fileKey() {
            return null
        }
    }

    public static exists(Path workdir, Path file, LinkOption option ){

        log.trace "Check if file exists for $workdir file: $file"

        //TODO ignore link
        Path workdirFile = Paths.get( workdir.toString() + File.separatorChar + ".command.outfiles" )
        return Files.lines(workdirFile)
                .parallel()
                .anyMatch {line ->
                    String[] data = line.split(';')
                    int i = 0
                    String currentPath = data[i++]
                    if( currentPath.startsWith('\'') && currentPath.endsWith('\'') ) {
                        currentPath = currentPath.substring( 1, currentPath.length() - 1 )
                    }

                    Path path = Paths.get(currentPath)
                    log.trace "Compare $path and $file match: ${path.toString() == file.toString()}"
                    return path.toString() == file.toString()
                }

    }

}
