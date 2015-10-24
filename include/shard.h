#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <kos_client.h>
#include <semaphore.h>
#include <hash.h>


typedef struct lst_item{
    KV_t pair;
    struct lst_item *next;


}LstItem;

typedef struct hash{
    LstItem* lst_item;
}Hash;

typedef struct shard{
  Hash hash[HT_SIZE];
  pthread_mutex_t mutex;
}Shard;
  

Shard* kos;


int n_shards;

void *server_thread(void *arg);
char* shard_get(int shardId, char* key);
char* shard_put(int shardId, char* key, char* value);
char* shard_remove( int shardId, char* key);
KV_t* shard_getAllKeys(int shardId, int dim) ;