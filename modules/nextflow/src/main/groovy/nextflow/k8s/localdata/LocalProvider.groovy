package nextflow.k8s.localdata

import org.apache.commons.lang.NotImplementedException

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

class LocalProvider extends FileSystemProvider {

    static LocalProvider INSTANCE = new LocalProvider()

    @Override
    String getScheme() {
        return "file"
    }

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        throw new NotImplementedException()
    }

    @Override
    Path getPath(URI uri) {
        throw new NotImplementedException()
    }

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void delete(Path path) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        throw new NotImplementedException()
    }

    @Override
    <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        if ( path instanceof LocalPath ) {
            return path.getAttributes()
        } else {
            Files.readAttributes(path, BasicFileAttributes.class)
        }
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new NotImplementedException()
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new NotImplementedException()
    }

    InputStream newInputStream(Path path, OpenOption... options) {
        return path.newInputStream()
    }

}
