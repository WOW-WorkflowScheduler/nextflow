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

    static final int VIRTUAL_PATH = 0
    static final int FILE_EXISTS = 1
    static final int REAL_PATH = 2
    static final int SIZE = 3
    static final int FILE_TYPE = 4
    static final int CREATION_DATE = 5
    static final int ACCESS_DATE = 6
    static final int MODIFICATION_DATE = 7

    public static TriFunction createLocalPath;

    public static Path walkFileTree(Path start,
                                    Set<FileVisitOption> options,
                                    int maxDepth,
                                    FileVisitor<? super Path> visitor,
                                    Path workDir
        )
    {

        String parent = null
        boolean skip = false

        File file = new File( start.toString() + File.separatorChar + ".command.outfiles" )
        String line;
        file.withReader { reader ->
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(';')

                String path = data[ VIRTUAL_PATH ]

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

                FileAttributes attributes = new FileAttributes( data )

                Path p = createLocalPath.apply( Paths.get(path), attributes, workDir )
                if ( attributes.isDirectory() ) {
                    def visitDirectory = visitor.preVisitDirectory( p, attributes )
                    if( visitDirectory == FileVisitResult.SKIP_SUBTREE ){
                        skip = true
                        parent = path
                    }
                } else {
                    visitor.visitFile( p, attributes)
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
        private final String fileType
        private final FileTime creationDate
        private final FileTime accessDate
        private final FileTime modificationDate

        FileAttributes( String[] data ) {
            if ( data.length != 8 && data[ FILE_EXISTS ] != "0" ) throw new RuntimeException( "Cannot parse row (8 columns required): ${data.join(',')}" )
            boolean fileExists = data[ FILE_EXISTS ] == "1"
            if ( data.length != 8 ) {
                this.link = true
                this.size = 0
                this.fileType = null
                this.creationDate = null
                this.accessDate = null
                this.modificationDate = null
                return
            }
            this.link = data[ REAL_PATH ].isEmpty()
            this.size = data[ SIZE ] as Long
            this.fileType = data[ FILE_TYPE ]
            this.creationDate = fileTimeFromString(data[ CREATION_DATE ])
            this.accessDate = fileTimeFromString(data[ ACCESS_DATE ])
            this.modificationDate = fileTimeFromString(data[ MODIFICATION_DATE ])
            this.directory = fileType == 'directory'
            if ( !directory && !fileType.contains( 'file' ) ){
                log.error( "Unknown type: $fileType" )
            }
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
                    Path currentPath = data[ VIRTUAL_PATH ] as Path
                    log.trace "Compare $currentPath and $fakePath match: ${(currentPath == fakePath)}"
                    if ( currentPath == fakePath ) {
                        Path p = data[ REAL_PATH ] ? data[ REAL_PATH ] as Path : currentPath
                        return createLocalPath.apply( p, new FileAttributes( data ), workDir )
                    } else return null
                }
                .filter{ it != null }
                .findFirst()

        return first.isPresent() ? first.get() : null

    }

    static interface TriFunction {
        Path apply( Path path, FileAttributes attributes, Path workDir );
    }

}
