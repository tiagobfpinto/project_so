#include <limits.h>
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
#include <sys/wait.h> // For wait
#include "operations.h"
#include <pthread.h>

const char *DIRECTORY;

static char **queue = NULL; // Pointer to the queue
static int queueSize = 0;   // Number of elements in the queue

// Mutex for queue operations
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

void process_job_file(const char *job_file);

void enqueue(char *element) {
    pthread_mutex_lock(&queue_mutex);
    char **temp = realloc(queue, (size_t)(queueSize + 1) * sizeof(char *));
    if (!temp) {
        perror("Memory allocation failed for queue");
        pthread_mutex_unlock(&queue_mutex);
        exit(1);
    }
    queue = temp;
    queue[queueSize++] = element;
    pthread_mutex_unlock(&queue_mutex);
}

char *dequeue() {
    pthread_mutex_lock(&queue_mutex);
    if (queueSize == 0) {
        pthread_mutex_unlock(&queue_mutex);
        return NULL;
    }

    char *firstElement = queue[0];

    // Shift pointers to the left
    for (int i = 1; i < queueSize; i++) {
        queue[i - 1] = queue[i];
    }

    queueSize--;

    char **temp = realloc(queue, (size_t)queueSize * sizeof(char *));
    if (queueSize > 0 && !temp) {
        perror("Memory reallocation failed for queue");
        pthread_mutex_unlock(&queue_mutex);
        exit(1);
    }
    queue = temp;
    pthread_mutex_unlock(&queue_mutex);

    return firstElement;
}

void freeQueue() {
    pthread_mutex_lock(&queue_mutex);
    for (int i = 0; i < queueSize; i++) {
        free(queue[i]); // Free each string in the queue
    }
    free(queue); // Free the queue itself
    queue = NULL;
    queueSize = 0;
    pthread_mutex_unlock(&queue_mutex);
}

void *thread_mission(void *arg) {
    (void)arg; // unused
    char *job = dequeue();
    if (job == NULL) {
        return NULL;
    }
    process_job_file(job);
    free(job);
    return NULL;
}

char **collect_jobs(size_t *file_count) {
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
            char **temp = realloc(job_files, (count + 1) * sizeof(char *));
            if (!temp) {
                perror("Memory allocation failed");
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
  
    *file_count = count;
    return job_files;
}

// Function to compare strings for sorting
int compare(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}



void process_queue(int MAX_THREADS) {
    pthread_t tid_array[MAX_THREADS];

    // Create up to MAX_THREADS threads
    for (int i = 0; i < MAX_THREADS; i++) {
        if (pthread_create(&tid_array[i], NULL, thread_mission, NULL) != 0) {
            perror("Failed to create thread");
            // If failed, no thread to join at this index
            continue;
        }
    }

    // Join all threads
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_join(tid_array[i], NULL);
    }
}


int handleBackup(const char *job_file) {
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
        fprintf(stderr, "Usage: %s <DIRECTORY> <max backups> <max threads>\n", argv[0]);
        return 1;
    }

    DIRECTORY = argv[1];
    max_backups = atoi(argv[2]);
    int MAX_THREADS = atoi(argv[3]);

    DIR *dir = opendir(DIRECTORY);
    if (!dir) {
        perror("Error opening DIRECTORY");
        return 1;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        closedir(dir);
        return 1;
    }
    closedir(dir);

    size_t file_count = 0;
    char **jobs = collect_jobs(&file_count);
    if (!jobs) {
        fprintf(stderr, "Failed to fill the queue with jobs\n");
        // If collect_jobs() returned NULL, no jobs were allocated, so no leak here.
        kvs_terminate();
        return 1;
    }

    // Enqueue all collected jobs
    for (size_t i = 0; i < file_count; i++) {
        enqueue(jobs[i]);
    }
    // Free the jobs array of pointers now that they've been enqueued
    free(jobs);

    // Process queue with MAX_THREADS threads
    process_queue(MAX_THREADS);

    // Wait for all backups to finish (if you're using current_backups)
    while (current_backups > 0) {
        wait(NULL);
        current_backups--;
    }

    kvs_terminate();
    

    freeQueue();

    return 0;
}