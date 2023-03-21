package nextflow.k8s.localdata

import org.apache.commons.lang.NotImplementedException

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

class LocalFileSystem extends FileSystem {

    static LocalFileSystem INSTANCE = new LocalFileSystem()

    @Override
    FileSystemProvider provider() {
        LocalProvider.INSTANCE
    }

    @Override
    void close() throws IOException {
        throw new NotImplementedException()
    }

    @Override
    boolean isOpen() {
        throw new NotImplementedException()
    }

    @Override
    boolean isReadOnly() {
        throw new NotImplementedException()
    }

    @Override
    String getSeparator() {
        throw new NotImplementedException()
    }

    @Override
    Iterable<Path> getRootDirectories() {
        throw new NotImplementedException()
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new NotImplementedException()
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        throw new NotImplementedException()
    }

    @Override
    Path getPath(String first, String... more) {
        throw new NotImplementedException()
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new NotImplementedException()
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new NotImplementedException()
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new NotImplementedException()
    }
}
