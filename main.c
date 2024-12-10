#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"


void process_job_file(const char *job_file,const char *output_file) {

    int fh;
    struct stat v;

    // Open the job file in read-only mode
    fh = open(job_file, O_RDONLY);
    if (fh == -1) {
        perror("Error opening the .job file");
        return;
    }

    // Use stat() to find the size of the file
    if (stat(job_file, &v) == -1) {
        perror("Error getting file size");
        close(fh);
        return;
    }

    // Check if file size is negative
    if (v.st_size < 0) {
        fprintf(stderr, "Invalid file size: %ld\n", v.st_size);
        close(fh);
        return;
    }
      

    // Start processing commands from the file
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    while (1) {
        // Fetch the next command from the file
        enum Command cmd = get_next(fh);
      

        switch (cmd) {
            case CMD_WRITE:
          
                num_pairs = parse_write(fh, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid WRITE command in file: %s\n", job_file);
                    continue;
                }
               
                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pairs in file: %s\n", job_file);
                }
                break;

            case CMD_READ:
             
                num_pairs = parse_read_delete(fh, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid READ command in file: %s\n", job_file);
                    continue;
                }
               
                if (kvs_read(num_pairs, keys,output_file)) {
                    fprintf(stderr, "Failed to read keys in file: %s\n", job_file);
                }
                break;

            case CMD_DELETE:
            
                num_pairs = parse_read_delete(fh, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid DELETE command in file: %s\n", job_file);
                    continue;
                }
                
                if (kvs_delete(num_pairs, keys,output_file)) {
                    fprintf(stderr, "Failed to delete keys in file: %s\n", job_file);
                }
                break;

            case CMD_SHOW:
                
                kvs_show(output_file);
                break;

            case CMD_WAIT:
            
                if (parse_wait(fh, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid WAIT command in file: %s\n", job_file);
                    continue;
                }
                printf("\nWaiting for %u ms.\n", delay);
                kvs_wait(delay);
                break;

            case CMD_BACKUP:
                
                if (kvs_backup()) {
                    fprintf(stderr, "Failed to perform backup in file: %s\n", job_file);
                }
                break;

            case CMD_HELP:
                
                printf(
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_INVALID:
                fprintf(stderr, "Invalid command in file: %s\n", job_file);
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                close(fh); // Clean up resources
                return;

            default:
                fprintf(stderr, "Unknown command in file: %s\n", job_file);
                break;
        }
    }
    

}




int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <directory> <max backups>\n", argv[0]);
        return 1;
    }

    const char *directory = argv[1];
    DIR *dir = opendir(directory); // abrir diretorio do parametro argv (jobs)
    if (!dir) {
        perror("Error opening directory");
        return 1;
    }

    // Initialize KVS
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        closedir(dir);
        return 1;
    }

    // Process all .job files in the directory
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job")) {
            char job_path[1024], out_path[1024];
            snprintf(job_path, sizeof(job_path), "%s/%s", directory, entry->d_name);

            // Create .out file path
            snprintf(out_path, sizeof(out_path), "%s/%s", directory, entry->d_name);
            char *dot = strrchr(out_path, '.');
            if (dot != NULL) {
                strcpy(dot, ".out"); // Replace ".job" with ".out"
            } else {
                strcat(out_path, ".out"); // Fallback if no extension found
            }

            // Create empty .out file
            int fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd == -1) {
                perror("Failed to create .out file");
            } else {
                close(fd); // Close the file immediately after creating it
            }

            // Process the .job file
            process_job_file(job_path,out_path);
        }
    }

    closedir(dir);

    // Enter main command loop (standard input)
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        printf("> ");
        fflush(stdout);

        switch (get_next(STDIN_FILENO)) {
            case CMD_WRITE:
                num_pairs = parse_write(STDIN_FILENO, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }
                break;

            case CMD_READ:
                num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_read(num_pairs, keys,NULL)) {
                    fprintf(stderr, "Failed to read pair\n");
                }
                break;

            case CMD_DELETE:
                num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (kvs_delete(num_pairs, keys,NULL)) { // parametro output file NULL porque estamos em modo de user input
                    fprintf(stderr, "Failed to delete pair\n");
                }
                break;

            case CMD_SHOW:
                kvs_show(NULL);
                break;

            case CMD_WAIT:
                if (parse_wait(STDIN_FILENO, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }
                if (delay > 0) {
                    printf("Waiting...\n");
                    kvs_wait(delay);
                }
                break;

            case CMD_BACKUP:
                if (kvs_backup()) {
                    fprintf(stderr, "Failed to perform backup\n");
                }
                break;

            case CMD_INVALID:
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                break;

            case CMD_HELP:
                printf(
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                kvs_terminate();
                return 0;
        }
    }

    return 0;
}