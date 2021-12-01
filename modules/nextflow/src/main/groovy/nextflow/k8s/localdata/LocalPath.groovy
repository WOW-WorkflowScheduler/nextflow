package nextflow.k8s.localdata

import groovy.util.logging.Slf4j
import nextflow.file.LocalFileWalker
import nextflow.k8s.client.K8sSchedulerClient
import org.codehaus.groovy.runtime.IOGroovyMethods
import sun.net.ftp.FtpClient

import java.nio.charset.Charset
import java.nio.file.*

@Slf4j
class LocalPath implements Path {

    private final Path path
    private final LocalFileWalker.FileAttributes attributes;
    private final K8sSchedulerClient client
    private boolean wasDownloaded = false

    private LocalPath(Path path, K8sSchedulerClient client, LocalFileWalker.FileAttributes attributes) {
        this.path = path
        this.client = client
        this.attributes = attributes
    }

    LocalPath toLocalPath( Path path, LocalFileWalker.FileAttributes attributes = null ){
        toLocalPath( path, client, attributes )
    }

    static LocalPath toLocalPath( Path path, K8sSchedulerClient client, LocalFileWalker.FileAttributes attributes ){
        ( path instanceof  LocalPath ) ? path as LocalPath : new LocalPath( path, client, attributes )
    }

    FtpClient getConnection( final String node, String daemon ){
        int trial = 0
        while ( true ) {
            try {
                FtpClient ftpClient = FtpClient.create(daemon)
                ftpClient.login("ftp", "nextflowClient".toCharArray() )
                ftpClient.enablePassiveMode( true )
                return ftpClient
            } catch ( IOException e ) {
                if ( trial > 5 ) throw e
                log.error("Cannot create FTP client: $daemon on $node", e)
                sleep(Math.pow(2, trial++) as long)
                daemon = client.getDaemonOnNode(node)
            }
        }
    }

    String getText(){
        getText( Charset.defaultCharset().toString() )
    }

    String getText( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.getText( charset )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return fileStream.getText( charset )
            }
        }
    }

    byte[] getBytes(){
        getText( Charset.defaultCharset().toString() )
    }

    byte[] getBytes( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.getBytes()
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return fileStream.getBytes()
            }
        }
    }

    Object withReader( Closure closure ){
        withReader ( Charset.defaultCharset().toString(), closure )
    }

    Object withReader( String charset, Closure closure ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.withReader( charset, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.withReader(fileStream, closure)
            }
        }
    }

    List<String> readLines(){
        readLines( Charset.defaultCharset().toString() )
    }

    List<String> readLines( String charset ){
        List<String> lines = new LinkedList<>()
        withReader( charset, { line -> lines.add( it as String )} )
        return lines
    }

    public <T> T eachLine( Closure<T> closure ) throws IOException {
        eachLine( Charset.defaultCharset().toString(), 1, closure )
    }

    public <T> T eachLine( int firstLine, Closure<T> closure ) throws IOException {
        eachLine( Charset.defaultCharset().toString(), firstLine, closure )
    }

    public <T> T eachLine( String charset, Closure<T> closure ) throws IOException {
        eachLine( charset, 1, closure )
    }

    public <T> T eachLine( String charset, int firstLine, Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachLine( charset, firstLine, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachLine( fileStream, charset, firstLine, closure )
            }
        }
    }

    BufferedReader newReader(){
        return newReader( Charset.defaultCharset().toString() )
    }

    BufferedReader newReader( String charset ){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.newReader()
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            InputStream fileStream = ftpClient.getFileStream(absolutePath)
            log.trace("Read remote $absolutePath")
            InputStreamReader isr = new InputStreamReader( fileStream, charset )
            return new BufferedReader(isr)
        }
    }

    public <T> T eachByte( Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachByte( closure )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachByte( new BufferedInputStream( fileStream ), closure)
            }
        }
    }

    public <T> T eachByte( int bufferLen, Closure<T> closure ) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.eachByte( bufferLen, closure )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                log.trace("Read remote $absolutePath")
                return IOGroovyMethods.eachByte( new BufferedInputStream( fileStream ), bufferLen, closure);
            }
        }
    }

    public <T> T withInputStream( Closure<T> closure) throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.withInputStream( closure )
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            InputStream fileStream = ftpClient.getFileStream(absolutePath)
            log.trace("Read remote $absolutePath")
            return IOGroovyMethods.withStream(new BufferedInputStream( fileStream ), closure)
        }
    }

    public BufferedInputStream newInputStream() throws IOException {
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        if ( wasDownloaded || location.sameAsEngine ){
            log.trace("Read locally $absolutePath")
            return path.newInputStream()
        }
        try (FtpClient ftpClient = getConnection( location.node, location.daemon )) {
            InputStream fileStream = ftpClient.getFileStream(absolutePath)
            log.trace("Read remote $absolutePath")
            return new BufferedInputStream( fileStream )
        }
    }

    private boolean download(){
        final String absolutePath = path.toAbsolutePath().toString()
        final def location = client.getFileLocation( absolutePath )
        synchronized ( this ) {
            if ( this.wasDownloaded || location.sameAsEngine ) {
                log.trace("No download")
                return false
            }
            try (FtpClient ftpClient = getConnection(location.node, location.daemon)) {
                try (InputStream fileStream = ftpClient.getFileStream(absolutePath)) {
                    log.trace("Download remote $absolutePath")
                    final def file = toFile()
                    path.parent.toFile().mkdirs()
                    OutputStream outStream = new FileOutputStream(file)
                    byte[] buffer = new byte[8 * 1024];
                    int bytesRead;
                    while ((bytesRead = fileStream.read(buffer)) != -1) {
                        outStream.write(buffer, 0, bytesRead);
                    }
                    fileStream.closeQuietly()
                    outStream.closeQuietly()
                    this.wasDownloaded = true
                    return true
                } catch (Exception e) {
                    throw e;
                }
            } catch (Exception e) {
                throw e;
            }
        }
    }

    @Override
    Object invokeMethod(String name, Object args) {
        boolean wasDownloaded = download()
        def file = path.toFile()
        def lastModified = file.lastModified();
        Object result = path.invokeMethod(name, args)
        if( lastModified != file.lastModified() ){
            //Update location in scheduler (overwrite all others)
            client.addFileLocation( path.toString(), file.size(), file.lastModified(), true )
        } else if ( wasDownloaded ){
            //Add location to scheduler
            client.addFileLocation( path.toString(), file.size(), file.lastModified(), false )
        }
        return result
    }

    boolean isDirectory( LinkOption... options ) {
        attributes ? attributes.isDirectory() : 0
    }

    long size() {
        attributes ? attributes.size() : 0
    }

    boolean empty(){
        //TODO empty file?
        this.size() == 0
    }

    boolean asBoolean(){
        true
    }

    @Override
    FileSystem getFileSystem() {
        path.getFileSystem()
    }

    @Override
    boolean isAbsolute() {
        path.isAbsolute()
    }

    @Override
    Path getRoot() {
        toLocalPath( path.getRoot() )
    }

    @Override
    Path getFileName() {
        toLocalPath( path.getFileName() )
    }

    @Override
    Path getParent() {
        toLocalPath( path.getParent() )
    }

    @Override
    int getNameCount() {
        path.getNameCount()
    }

    @Override
    Path getName(int index) {
        path.getName( index )
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        toLocalPath( path.subpath( beginIndex, endIndex ) )
    }

    @Override
    boolean startsWith(Path other) {
        path.startsWith( other )
    }

    @Override
    boolean startsWith(String other) {
        path.startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        path.endsWith( other )
    }

    @Override
    boolean endsWith(String other) {
        path.endsWith( other )
    }

    @Override
    Path normalize() {
        toLocalPath( path.normalize() )
    }

    @Override
    Path resolve(Path other) {
        toLocalPath( path.normalize() )
    }

    @Override
    Path resolve(String other) {
        path.resolve( other )
    }

    @Override
    Path resolveSibling(Path other) {
        path.resolveSibling( other )
    }

    @Override
    Path resolveSibling(String other) {
        path.resolveSibling( other )
    }

    @Override
    Path relativize(Path other) {
        path.relativize( other )
    }

    @Override
    URI toUri() {
        path.toUri()
    }

    Path toAbsolutePath(){
        toLocalPath( path.toAbsolutePath() )
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        toLocalPath( path.toRealPath( options ) )
    }

    @Override
    File toFile() {
        path.toFile()
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        path.register( watcher, events, modifiers )
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        path.register( watcher, events )
    }

    @Override
    int compareTo(Path other) {
        path.compareTo( other)
    }

    @Override
    String toString() {
        path.toString()
    }
}
