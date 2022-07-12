#include <fts.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define FULL_DESCR 0
#define SHORT_DESCR_WITH_TIMESTAMP 1

int collectFileInformation(char * const argv[], const int version,
    const char * const local_dir,
    const char * const result_filename);
int getFullDescr(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr);
int getShortDescrAndTimestamp(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr);


int main(int arc, char * const argv[]) {
    if (arc < 5) {
        return -1;
    }
    argv++;
    const int version = strcmp(argv++[0], "short") == 0
        ? SHORT_DESCR_WITH_TIMESTAMP : FULL_DESCR;
    const char * const result_filename = argv++[0];
    const char * const local_dir = argv++[0];
    int rc = collectFileInformation(argv, version, local_dir, result_filename);
    return rc;
}

int collectFileInformation(char * const * dir_to_search, const int version,
    const char * const local_dir, 
    const char * const result_filename) {

    FILE * file_ptr = fopen(result_filename, "w");
    if (file_ptr == NULL) {
        printf("Error opening the file %s\n", result_filename);
        return -1;
    }
    int rc;
    switch (version) {
        case FULL_DESCR:
            rc = getFullDescr(dir_to_search, local_dir, file_ptr);
            break;
        case SHORT_DESCR_WITH_TIMESTAMP:
            rc = getShortDescrAndTimestamp(dir_to_search, local_dir, file_ptr);
            break;
        
        default:
            break;
    }
    fclose(file_ptr);
    return rc;
}

int getFullDescr(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr) {

    FTS * fts_ptr;
    FTSENT * ptr, * ch_ptr;
    int fts_options = FTS_PHYSICAL;

    if ((fts_ptr = fts_open(dir, fts_options, NULL)) == NULL) {
        printf("Error traversing the directory %s\n", *dir);
        return -1;
    }
    ch_ptr = fts_children(fts_ptr, 0);
    if (ch_ptr == NULL) {
        return 0;
    }
    int skip_next = 0;
    while ((ptr = fts_read(fts_ptr)) != NULL) {
        if (ptr->fts_info == FTS_DP) {
            continue;
        }
        if (skip_next) {
            skip_next = 0;
            continue;
        }
        char * file_type;
        char * symlink_target_path = "";
        int exists;
        switch (ptr->fts_info) {
            case FTS_D:
                file_type = "directory";
                exists = 1;
                break;
            case FTS_F:
                file_type = "regular file";
                exists = 1;
                break;
            case FTS_SL:
                file_type = "symbolic link";
                symlink_target_path = realpath(ptr->fts_path, NULL);
                if (symlink_target_path == NULL) {
                    symlink_target_path = "";
                }
                exists = 1 + access(symlink_target_path, F_OK); // test for file existence
                break;

            default:
                break;
        }
        fprintf(
            file_ptr,
            "%s;%i;%s;%li;%s;%li%li;%li%li;%li%li\n",
            ptr->fts_path,
            exists,
            symlink_target_path,
            ptr->fts_statp->st_size,
            file_type,
            ptr->fts_statp->st_ctim.tv_sec, ptr->fts_statp->st_ctim.tv_nsec,
            // ctim - time of last status change which is used as an approximation of the creation time
            ptr->fts_statp->st_atim.tv_sec, ptr->fts_statp->st_atim.tv_nsec,
            ptr->fts_statp->st_mtim.tv_sec, ptr->fts_statp->st_mtim.tv_nsec
        );
        // if a symlink points to a directory, we continue searching that directory
        if (symlink_target_path != "") {
            // if target is not local, we skip it
            if (strncmp(symlink_target_path, local_dir, strlen(local_dir)) != 0) {
                continue;
            }
            // if target is within the directory we are searching, we skip it,
            // to prevent searching a directory more than once
            if (strncmp(symlink_target_path, *dir, strlen(*dir)) == 0) {
                continue;
            }
            // checking whether it is a directory:
            struct stat target_file_stat;
            int stat_rv;
            if ((stat_rv = stat(symlink_target_path, &target_file_stat)) != 0) {
                printf("Error reading the file %s\n", symlink_target_path);
                return stat_rv;
            }
            if (S_ISDIR(target_file_stat.st_mode)) {
                fts_set(fts_ptr, ptr, FTS_FOLLOW);
                skip_next = 1;
            }
        }
    }
    fts_close(fts_ptr);
    return 0;
}

int getShortDescrAndTimestamp(char * const * dir,
    const char * const local_dir,
    FILE * file_ptr) {

    struct timespec time_now;
    int gettimeofday_rc;
    if ((gettimeofday_rc = clock_gettime(CLOCK_REALTIME, &time_now)) != 0) {
        printf("Error getting the time of day\n");
        return gettimeofday_rc;
    } 
    fprintf(file_ptr, "%li%li\n", time_now.tv_sec, time_now.tv_nsec);

    fprintf(file_ptr, "%s\n", *dir);

    FTS * fts_ptr;
    FTSENT * ptr, * ch_ptr;
    int fts_options = FTS_PHYSICAL;

    if ((fts_ptr = fts_open(dir, fts_options, NULL)) == NULL) {
        printf("Error traversing the directory %s\n", *dir);
        return -1;
    }
    ch_ptr = fts_children(fts_ptr, 0);
    if (ch_ptr == NULL) {
        return 0;
    }
    int skip_next = 0;
    while ((ptr = fts_read(fts_ptr)) != NULL) {
        if (ptr->fts_info == FTS_DP) {
            continue;
        }
        if (skip_next) {
            skip_next = 0;
            continue;
        }
        char * file_type;
        char * symlink_target_path = "";
        int exists;
        switch (ptr->fts_info) {
            case FTS_D:
                file_type = "directory";
                exists = 1;
                break;
            case FTS_F:
                file_type = "regular file";
                exists = 1;
                break;
            case FTS_SL:
                file_type = "symbolic link";
                symlink_target_path = realpath(ptr->fts_path, NULL);
                if (symlink_target_path == NULL) {
                    symlink_target_path = "";
                }
                exists = 1 + access(symlink_target_path, F_OK); // test for file existence
                break;

            default:
                break;
        }
        fprintf(
            file_ptr,
            "%s;%i;%s;%li;%s\n",
            ptr->fts_path,
            exists,
            symlink_target_path,
            ptr->fts_statp->st_size,
            file_type
        );
        // if a symlink points to a directory, we continue searching that directory
        if (symlink_target_path != "") {
            // if target is not local, we skip it
            if (strncmp(symlink_target_path, local_dir, strlen(local_dir)) != 0) {
                continue;
            }
            // if target is within the directory we are searching, we skip it,
            // to prevent searching a directory more than once
            if (strncmp(symlink_target_path, *dir, strlen(*dir)) == 0) {
                continue;
            }
            // checking whether it is a directory:
            struct stat target_file_stat;
            int stat_rv;
            if ((stat_rv = stat(symlink_target_path, &target_file_stat)) != 0) {
                printf("Error reading the file %s\n", symlink_target_path);
                return stat_rv;
            }
            if (S_ISDIR(target_file_stat.st_mode)) {
                fts_set(fts_ptr, ptr, FTS_FOLLOW);
                skip_next = 1;
            }
        }
    }
    fts_close(fts_ptr);
    return 0;
}
