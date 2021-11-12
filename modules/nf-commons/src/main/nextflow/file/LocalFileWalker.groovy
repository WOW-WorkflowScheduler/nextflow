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
import java.text.SimpleDateFormat

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
                if ( data.length != 7 ) throw new RuntimeException( "Can not parse row (7 columns required): \"" + line + "\"" )
                String path = data[i++]
                String symlinkTo = data[i++]
                Long bytes = data[i++] as Long
                String fileType = data[i++]
                FileTime creationDate = fileTimeFromString(data[i++])
                FileTime accessDate = fileTimeFromString(data[i++])
                FileTime modificationDate = fileTimeFromString(data[i++])

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

                Boolean link = symlinkTo ? true : false

                if (fileType == 'directory') {
                    FileAttributes attributes = new FileAttributes(
                            true, link, bytes, creationDate, accessDate, modificationDate
                    )
                    def visitDirectory = visitor.preVisitDirectory(Paths.get(path), attributes);
                    if( visitDirectory == FileVisitResult.SKIP_SUBTREE ){
                        skip = true
                        parent = path
                    }
                } else if ( fileType.contains( 'file' ) ) {
                    FileAttributes attributes = new FileAttributes(
                            false, link, bytes, creationDate, accessDate, modificationDate
                    )
                    visitor.visitFile(Paths.get(path), attributes)
                } else {
                    log.error( "Unknown type: $fileType" )
                }
            }
        }

        return start
    }

    private static FileTime fileTimeFromString(String date) {
        // date has the format "2021-11-02 08:49:30.955691861 +0000"
        if( !date || date == "-" ) {
            return null
        }
        String[] parts = date.split(" ")
        parts[1] = parts[1].substring(0, 12)
        // parts[1] now has milliseconds as smallest units e.g. "08:49:30.955"
        String shortenedDate = String.join(" ", parts)
        long millis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z").parse(shortenedDate).getTime()
        return FileTime.fromMillis(millis)
    }

    static class FileAttributes implements BasicFileAttributes {

        private final boolean directory
        private final boolean link
        private final long size
        private final FileTime creationDate
        private final FileTime accessDate
        private final FileTime modificationDate

        FileAttributes(
                Boolean directory,
                Boolean link,
                Long size,
                FileTime creationDate,
                FileTime accessDate,
                FileTime modificationDate
        ) {
            this.directory = directory
            this.link = link
            this.size = size
            this.creationDate = creationDate
            this.accessDate = accessDate
            this.modificationDate = modificationDate
        }

        @Override
        FileTime lastModifiedTime() {
            return modificationDate
        }

        @Override
        FileTime lastAccessTime() {
            return accessDate
        }

        @Override
        FileTime creationTime() {
            return creationDate
        }

        @Override
        boolean isRegularFile() {
            return !directory
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

    static Path exists( final File outfile, final Path file, final Path workDir, final LinkOption option ){

        String rootDirString
        Scanner sc = new Scanner( outfile )
        if( sc.hasNext() )
            rootDirString = sc.next().split(";")[0]
        else
            throw new IllegalStateException( "Outputfile is empty: " + outfile )

        final Path rootDir = rootDirString as Path
        final Path fakePath = FileHelper.fakePath( file, rootDir )

        //TODO ignore link
        final Optional<Path> first = Files.lines( outfile.toPath() )
                .parallel()
                .map {line ->
                    String[] data = line.split(';')
                    Path currentPath = data[0] as Path
                    log.trace "Compare $currentPath and $fakePath match: ${(currentPath == fakePath)}"
                    if ( currentPath == fakePath ) {
                        return data[1] ? data[1] as Path : currentPath
                    } else return null
                }
                .filter{ it != null }
                .findFirst()

        return first.isPresent() ? first.get() : null

    }

}
