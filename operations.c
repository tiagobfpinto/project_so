#include <stdio.h>

#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int write_to_file(const char *output_file, char output[MAX_STRING_SIZE], int newline){
  int fd = open(output_file, O_WRONLY | O_APPEND | O_CREAT, 0644);
      if (fd == -1) {
        perror("Failed to open output file");
        return 0;
    }
      if(newline ==2){
    
        write(fd,"\n",1);
        }
      write(fd, output, strlen(output));
      if(newline ==1){write(fd,"\n",1);}
      
      
      close(fd);
    return 1;
}


int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int compare_keys(const void *a, const void *b) {
    const char *keyA = (const char *)a;
    const char *keyB = (const char *)b;
    return strcmp(keyA, keyB);
}


int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], const char *output_file) {
  
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }
    // Sort the keys alphabetically before processing
    qsort(keys, num_pairs, MAX_STRING_SIZE, compare_keys);

    // Dynamically sized buffer: 
    // For each pair, we will have at most "(key,value)" of size ~ 2*MAX_STRING_SIZE plus commas, parentheses, etc.
    // To be safe, allocate enough space: 
    //   - Each pair adds up to: 2 * MAX_STRING_SIZE (worst case) + some overhead (5 bytes for "(),," etc.)
    //   - num_pairs * (2*MAX_STRING_SIZE + 5) is a safe upper bound
    size_t buffer_size = num_pairs * (2 * MAX_STRING_SIZE + 5) + 3; 
    char *read_output = malloc(buffer_size);
   
    strcpy(read_output, "["); //adiciona o placeholder inicial à string output 

    for (size_t i = 0; i < num_pairs; i++) {
        char temp[MAX_STRING_SIZE];
        char *result = read_pair(kvs_table, keys[i]);

        if (result == NULL) {
            snprintf(temp, sizeof(temp), "(%s,KVSERROR)", keys[i]);
        } else {
            snprintf(temp, sizeof(temp), "(%s,%s)", keys[i], result);
            free(result);
        }

        strcat(read_output, temp);
        
    }

    strcat(read_output, "]"); // Close the JSON-like format

    if(output_file != NULL ){ //file input mode
      write_to_file(output_file,read_output,1);
      
    }
  

    printf("%s\n", read_output); // Print the output
    free(read_output);
    return 0;
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE],const char *output_file) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  char output[MAX_STRING_SIZE] = "";
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        strcat(output,"[");
        
        aux = 1;
      }
      char temp[MAX_STRING_SIZE];
      
      snprintf(temp,sizeof(temp),"(%s,KVSMISSING)", keys[i]);
      strcat(output,temp);
   
    }
  }
  if (aux) {
    strcat(output,"]");
  }
  printf("%s", output); 
  if(output_file != NULL){
    write_to_file(output_file,output,1);
  }

  return 0;
}

void kvs_show(const char *output_file) {
  int new_line = 2;
  int first_node = 1;
  char temp[MAX_STRING_SIZE];
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      if(first_node){
        first_node =0;
        new_line =0;
      }else{
        new_line = 2;
      }
      snprintf(temp,sizeof(temp),"(%s, %s)", keyNode->key, keyNode->value);
     


      if(output_file != NULL){ write_to_file(output_file,temp,new_line);}
      

      printf("(%s, %s)\n", keyNode->key, keyNode->value);


      keyNode = keyNode->next; // Move to the next node
      
      
    }

  }
 
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}