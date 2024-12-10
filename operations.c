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

int write_to_file(const char *output_file, char output[MAX_STRING_SIZE]){
  int fd = open(output_file, O_WRONLY | O_APPEND | O_CREAT, 0644);
      if (fd == -1) {
        perror("Failed to open output file");
        return 0;
    }
      write(fd, output, strlen(output));
      write(fd,"\n",1);
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



int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], const char *output_file) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }


    char read_output[MAX_STRING_SIZE] = "[";
    

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
        if (i < num_pairs - 1) {
            strcat(read_output, ","); // Add comma between pairs
        }
    }

    strcat(read_output, "]"); // Close the JSON-like format

    if(output_file != NULL ){ //file input mode
      write_to_file(output_file,read_output);
      
    }
  

    printf("%s\n", read_output); // Print the output
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
    write_to_file(output_file,output);
  }

  return 0;
}

void kvs_show(const char *output_file) {
  char temp[MAX_STRING_SIZE];
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      snprintf(temp,sizeof(temp),"(%s, %s)", keyNode->key, keyNode->value);
     
      if(output_file != NULL){ write_to_file(output_file,temp);}
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