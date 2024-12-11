#include <limits.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h> // ADDED for wait
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <pthread.h>

void process_job_file(const char *job_file);
const char *DIRECTORY;

//parametro para as threads
typedef struct thread_param_s {
     // each thread irá receber an um job file
    const char *job_file; //nome do job file
  
} thread_param_t;

void *thread(void *param) {
    thread_param_t *thread_param = (thread_param_t*)param;
    char *job = (char *)thread_param->job_file; // cast to non-const for free

    process_job_file(job);

    free(job);          // Now free the job file string
    free(thread_param); // and the param structure
    return NULL;
}


// Function to compare strings for sorting
int compare(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

char **collect_and_sort_jobs(size_t *file_count) {
    DIR *dir = opendir(DIRECTORY);
    if (!dir) {
        perror("Failed to open DIRECTORY");
        return NULL;
    }

    struct dirent *entry;
    char **job_files = NULL;
    size_t count = 0;

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job")) {
            // Use a temp pointer for realloc to avoid losing the original if it fails
            char **temp = realloc(job_files, (count + 1) * sizeof(char *));
            if (!temp) {
                perror("Memory allocation failed");
                // Free previously allocated strings
                for (size_t i = 0; i < count; i++) {
                    free(job_files[i]);
                }
                free(job_files);
                closedir(dir);
                return NULL;
            }
            job_files = temp;

            job_files[count] = strdup(entry->d_name);
            if (!job_files[count]) {
                perror("Memory allocation failed");
                // Free all previously allocated strings before returning
                for (size_t i = 0; i < count; i++) {
                    free(job_files[i]);
                }
                free(job_files);
                closedir(dir);
                return NULL;
            }
            count++;
        }
    }

    closedir(dir);

    // If we have files, sort them
    if (count > 0) {
        qsort(job_files, count, sizeof(char *), compare);
    }

    *file_count = count;
    return job_files;
}


void process_sorted_jobs(char **job_files, size_t file_count, int MAX_THREADS) {
    pthread_t tid_array[MAX_THREADS];
    size_t files_processed = 0;

    while (files_processed < file_count) {
        int active_threads = 0;

        for (int i = 0; i < MAX_THREADS && files_processed < file_count; i++) {
            thread_param_t *param = malloc(sizeof(thread_param_t));
            if (!param) {
                perror("Failed to allocate memory for thread parameter");
                exit(EXIT_FAILURE);
            }

            // Transfer ownership of the string to the thread
            param->job_file = job_files[files_processed];
            job_files[files_processed] = NULL;  // so we don't free it again in main
            files_processed++;

            if (pthread_create(&tid_array[active_threads], NULL, thread, param) != 0) {
                perror("Failed to create thread");
                free(param); // If creation fails, free param
                continue;
            }

            active_threads++;
        }

        for (int i = 0; i < active_threads; i++) {
            pthread_join(tid_array[i], NULL);
        }
    }

    // Free the array of pointers (some may be NULL if freed by threads)
    for (size_t i = 0; i < file_count; i++) {
        // Only free if not already freed by thread
        if (job_files[i]) {
            free(job_files[i]);
        }
    }
    free(job_files);
}

int handleBackup(
    const char *job_file) {
    //printf("NEED to create backup for: [%s] in DIRECTORY [%s]\n", job_file, DIRECTORY);

    char job_path[1024], out_path[2048];
    snprintf(job_path, sizeof(job_path), "%s/%s", DIRECTORY, job_file);

    // Extract just the filename (without directories)
    const char *filename = strrchr(job_file, '/');
    if (filename) {
        filename++; // skip '/'
    } else {
        filename = job_file;
    }

    // Remove ".job" extension from filename if present, but keep the base name
    char base_name[1024];
    strncpy(base_name, filename, sizeof(base_name));
    base_name[sizeof(base_name)-1] = '\0'; // safety null-termination

    char *dot = strrchr(base_name, '.');
    if (dot && strcmp(dot, ".job") == 0) {
        *dot = '\0'; // remove ".job"
    }

    // Now we attempt to find a backup filename that doesn't exist yet
    // Start from backup_count = 1
    int backup_count = 1;
    while (1) {
        snprintf(out_path, sizeof(out_path), "%s/%s-%d.bck", DIRECTORY, base_name, backup_count);
        
        // Check if file already exists
        if (access(out_path, F_OK) == -1) {
            // File does not exist, we can use this name
            break;
        }
        // If file exists, increment backup_count and try again
        backup_count++;
    }

    // Create empty .bck file
    //printf("Creating backup file: %s\n", out_path);
    int fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Failed to create .bck file");
        return 1;
    } else {
        close(fd); // Close after creation
    }

    // Now call kvs_backup with the chosen output_file
    if (kvs_backup(out_path) != 0) {
        fprintf(stderr, "Failed to perform backup on file: %s\n", out_path);
        return 1;
    }

    return 0;
}



void parse_job_file(const char *job_file,const char *output_file) {

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
                if(handleBackup(job_file)){
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

void process_job_file(const char *job_file) {
    printf("Processing job file: %s\n", job_file);

    char job_path[1024], out_path[1024];
    snprintf(job_path, sizeof(job_path), "%s/%s", DIRECTORY, job_file);

    // Create .out file path
    snprintf(out_path, sizeof(out_path), "%s/%s", DIRECTORY, job_file);
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

    // Process the job file
    parse_job_file(job_path, out_path);
    return;
}




int main(int argc, char *argv[]) {
   
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <DIRECTORY> <max backups><xxx>\n", argv[0]);
        return 1;
    }
    

   
    DIRECTORY = argv[1];
    max_backups = atoi(argv[2]); // ADDED: guardar max_backups
    int MAX_THREADS = atoi(argv[3]); // guardar max threads

    DIR *dir = opendir(DIRECTORY); // abrir diretorio do parametro argv (jobs)
    if (!dir) {
        perror("Error opening DIRECTORY");
        return 1;
    }

    // Initialize KVS
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        closedir(dir);
        return 1;
    }
    closedir(dir); // Close the directory after done

   
    size_t file_count = 0;
    char **job_files = collect_and_sort_jobs(&file_count);
    if (!job_files) {
        fprintf(stderr, "Failed to collect and sort job files\n");
        return 1;
    }


    process_sorted_jobs(job_files, file_count,MAX_THREADS);

     // Ao fim, aguardar todos os backups terminarem (opcional, mas recomendado)
    while (current_backups > 0) {
        wait(NULL);
        current_backups--;
    }
    kvs_terminate();

    return 0;
}