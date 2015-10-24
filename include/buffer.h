#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <kos_client.h>
#include <semaphore.h>

typedef struct {
 
  int pedido;/* 0-none, 1-get, 2-put, 3-remove,4-getall*/
  int shardId;
  int dim;
  char* key;
  char* value;
  KV_t* keyvalue;
  sem_t semaphore1;
  sem_t semaphore2;
}Bufferitem;

Bufferitem* buffer;
int buffer_size;