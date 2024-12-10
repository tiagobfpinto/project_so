#include "kvs.h"
#include "string.h"
#include <stdint.h>
#include <stdlib.h>
#include <ctype.h>


int table_size = TABLE_SIZE; 

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project

uint32_t fnv1a_hash(const char *key, size_t len) { // not used right now as we're using teacher's hashing for testing
    uint32_t hash = 2166136261u; // FNV offset basis
    const uint32_t fnv_prime = 16777619u;

    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)key[i];  // XOR with the current byte
        hash *= fnv_prime;        // Multiply by the FNV prime
    }

    return hash;
}

int hash_better(const char *key) {
    uint32_t hash_value = fnv1a_hash(key, strlen(key));
    return (int)(hash_value % (uint32_t)table_size); // Fix type mismatch
}

int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable* create_hash_table() { //inicialzia a hash table metendo todos os ponteiros como nulos
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < table_size; i++) {
      ht->table[i] = NULL;
  }
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) { //collision handling is working
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) { // collision handling
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return copy of the value if found
        }
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) { // collision handling
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }
    
    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < table_size; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    free(ht);
}