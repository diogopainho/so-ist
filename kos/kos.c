#include <kos_client.h>
#include <buffer.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <shard.h>
#include <string.h>
#include <delay.h>



int kos_init(int num_server_threads, int buf_size, int num_shards) {
    int i,j,s, *res;
  pthread_t* threads=(pthread_t*)malloc(sizeof(pthread_t)*num_server_threads);
  Shard *shards=(Shard*) malloc(sizeof(Shard)*num_shards);
  int* ids=(int*) malloc(sizeof(int)*num_server_threads);
  if(num_shards<0){
              printf("ERROR: kos_init: number of shards is not valid\n");
               return -1;
     }
      if(buf_size<0){
              printf("ERROR: kos_init: buffer size is not valid\n");
              return -1;
      }
      if(num_server_threads<0){
              printf("ERROR: kos_init: number of server threads is not valid\n");
              return -1;
      }
      if(buf_size!=num_server_threads){
              printf("ERROR: kos_init: buffer size is not equal to number of threads\n");
              return -1;
      }


buffer_size=buf_size;
n_shards=num_shards;
  buffer=(Bufferitem*)malloc(sizeof(Bufferitem)*buf_size);

    kos=shards;
    
    for(i=0; i<num_shards; i++){
      for(j=0; j<HT_SIZE;j++){
	kos[i].hash[j].lst_item=NULL;
      }
      pthread_mutex_init(&kos[i].mutex, NULL);
    }

  for(i=0; i<buf_size; i++){
    buffer[i].pedido=0;
    buffer[i].dim=0;
    buffer[i].key=NULL;
    buffer[i].value=NULL;

     
    if ((s=sem_init(&buffer[i].semaphore1, 0, 0)))  {
			printf("ERROR: kos_init: sem_init failed with code %d!\n",s);
			return -1;
		}        		

    if ((s=sem_init(&buffer[i].semaphore2, 0, 0)))  {
			printf("ERROR: kos_init: sem_init failed with code %d!\n",s);
			return -1;
		}
  }
 
  for (i=0; i<num_server_threads; i++) {	
		ids[i]=i;		
		
		if ( (s=pthread_create(&threads[i], NULL, &server_thread, &(ids[i])) ) ) {
			printf("ERROR: kos_init: pthread_create failed with code %d!\n",s);
			return -1;
		}
	}
	
	

	return 0;

}


/*--------------------------------------------------------*/

char* kos_get(int clientid, int shardId, char* key) {
  
	char* tvalue=(char*)malloc(sizeof(char)*20);
    if(clientid<0){
            printf("ERROR: kos_get: clientid is not valid\n");
            return NULL;
    }
    if(shardId<0||shardId>n_shards){
            printf("ERROR: kos_get: shardId is not valid\n");
            return NULL;
    }
    if(key==NULL){
      printf("ERROR  kos_get: key is null\n");
      return NULL;
    }
    
    if(strlen(key)>KV_SIZE){
        printf("ERROR  kos_get: key is too big\n");
      return NULL;
    }
      
	clientid=clientid%buffer_size;
	buffer[clientid].key=(char*)malloc(sizeof(char)*KV_SIZE);
       buffer[clientid].value=(char*)malloc(sizeof(char)*KV_SIZE);
	buffer[clientid].shardId=shardId;
	strcpy(buffer[clientid].key, key);
	buffer[clientid].pedido=1;
	sem_post(&buffer[clientid].semaphore1);
	sem_wait(&buffer[clientid].semaphore2);
	if(buffer[clientid].value==NULL)
	  return NULL;
        strcpy(tvalue, buffer[clientid].value);

	free(buffer[clientid].key);
       free(buffer[clientid].value);
	return tvalue;
}




char* kos_put(int clientid, int shardId, char* key, char* value) {
   
	
       char* tvalue=(char*)malloc(sizeof(char)*20);
       
	    if(clientid<0){
			  printf("ERROR: kos_put: clientid is not valid\n");
			  return NULL;
		  }    
	   if(shardId<0||shardId>n_shards){
	                printf("ERROR: kos_put: shardId is not valid\n");
	                return NULL;
	        }
	          if(key==NULL && value==NULL){
	          printf("ERROR  kos_put: key and value are null\n");
	          return NULL;
	        }
	        if(key==NULL){
	          printf("ERROR  kos_put: key is null\n");
	          return NULL;
	        }
	         if(value==NULL){
	          printf("ERROR  kos_put: value is null\n");
	          return NULL;
	        }
	        
	       if(strlen(key)>KV_SIZE){
		printf("ERROR  kos_put: key is too big\n");
		return NULL;
	      }
	      if(strlen(value)>KV_SIZE){
		printf("ERROR  kos_put: value is too big\n");
		return NULL;
	      }
       	clientid=clientid%buffer_size;

       buffer[clientid].key=(char*)malloc(sizeof(char)*KV_SIZE);
       buffer[clientid].value=(char*)malloc(sizeof(char)*KV_SIZE);
       
	buffer[clientid].shardId=shardId;
	strcpy(buffer[clientid].key, key);
	strcpy(buffer[clientid].value, value);
	buffer[clientid].pedido=2;
	sem_post(&buffer[clientid].semaphore1);
	sem_wait(&buffer[clientid].semaphore2);
	if(buffer[clientid].value==NULL)
	  return NULL;
        strcpy(tvalue, buffer[clientid].value);

	free(buffer[clientid].key);
       free(buffer[clientid].value);

	return tvalue;
}

char* kos_remove(int clientid, int shardId, char* key) {
        char* tvalue=(char*)malloc(sizeof(char)*20);
	clientid=clientid%buffer_size;
	
            if(clientid<0){
                    printf("ERROR: kos_remove: clientid is not valid\n");
                    return NULL;
            }
    if(shardId<0||shardId>n_shards){
            printf("ERROR: kos_remove: shardId is not valid\n");
            return NULL;
    }
     if(key==NULL){
      printf("ERROR: kos_remove: key is null\n");
      return NULL;
    }
	       if(strlen(key)>KV_SIZE){
		printf("ERROR  kos_remove: key is too big\n");
		return NULL;
	      }
	buffer[clientid].key=(char*)malloc(sizeof(char)*KV_SIZE);
       buffer[clientid].value=(char*)malloc(sizeof(char)*KV_SIZE);
	buffer[clientid].shardId=shardId;
	strcpy(buffer[clientid].key, key);
	buffer[clientid].pedido=3;
	sem_post(&buffer[clientid].semaphore1);
	sem_wait(&buffer[clientid].semaphore2);
	if(buffer[clientid].value==NULL)
	  return NULL;
        strcpy(tvalue, buffer[clientid].value);

       free(buffer[clientid].key);
       free(buffer[clientid].value);

	return tvalue;
}

KV_t* kos_getAllKeys(int clientid, int shardId, int* dim) {
  	if(clientid<0){
            printf("ERROR: kos_getAllKeys: clientid is not valid\n");
            return NULL;
    }
    if(shardId<0||shardId>n_shards){
            printf("ERROR: kos_getAllKeys: shardId is not valid");
            return NULL;
    }
    
	clientid=clientid%buffer_size;
	buffer[clientid].keyvalue=(KV_t*)malloc(sizeof(KV_t)*(*dim)); /*Buffer[i]*/
	buffer[clientid].shardId=shardId;
	buffer[clientid].pedido=4;
	buffer[clientid].dim=(*dim);
	sem_post(&buffer[clientid].semaphore1);
	sem_wait(&buffer[clientid].semaphore2);
        
	
	
	return buffer[clientid].keyvalue; /*Buffer[i]*/
}
/*--------------------------------------------------------*/


void *server_thread(void *arg) {
  int server_id=*( (int*)arg);
  while(1){
    sem_wait(&buffer[server_id].semaphore1);
    delay();
    pthread_mutex_lock(&kos[buffer[server_id].shardId].mutex);
    switch(buffer[server_id].pedido){
      
      case 1:
	buffer[server_id].value=shard_get(buffer[server_id].shardId, buffer[server_id].key);
	break;
      case 2:
	buffer[server_id].value=shard_put(buffer[server_id].shardId, buffer[server_id].key, buffer[server_id].value);
        break;
      case 3:
	buffer[server_id].value=shard_remove(buffer[server_id].shardId, buffer[server_id].key);
        break;
      case 4:
	buffer[server_id].keyvalue=shard_getAllKeys(buffer[server_id].shardId, buffer[server_id].dim); 
	break;
      default:
	        printf("Server_Thread: %d - Not valid option\n", buffer[server_id].pedido);
	break;

    
    
    }
        pthread_mutex_unlock(&kos[buffer[server_id].shardId].mutex);

    sem_post(&buffer[server_id].semaphore2);
    
    
    }
    return NULL;
}
    
/*-------------------------------------------------------*/


char* shard_get(int shardId, char* key) {
  char* tvalue =(char*)malloc(sizeof(char)*20);  
  int hash_pos=hash(key);
  LstItem *temp;

  
  if(kos[shardId].hash[hash_pos].lst_item == NULL){

        return NULL;
    } 
  
  temp=kos[shardId].hash[hash_pos].lst_item;

      while(temp != NULL){
	if(strcmp(key, temp->pair.key) == 0){	

	  strcpy(tvalue, temp->pair.value);
	  return tvalue;
	}
	temp=temp->next;
      }
 
    
    return NULL;
}





char* shard_put(int shardId, char* key, char* value) {
    char* tvalue =(char*)malloc(sizeof(char)*20);  
    LstItem *temp;
    int hash_pos=hash(key);
    LstItem *item=(LstItem*)malloc(sizeof(LstItem));
    strcpy(item->pair.value, value);
    strcpy(item->pair.key, key);
    item->next=NULL;
	
    
    
    if(kos[shardId].hash[hash_pos].lst_item == NULL){
        kos[shardId].hash[hash_pos].lst_item = item;

        return NULL;
    } 
  


      temp=kos[shardId].hash[hash_pos].lst_item;

      while(temp != NULL){
	if(strcmp(key, temp->pair.key) == 0){	

	  strcpy(tvalue, temp->pair.value);
	  strcpy(temp->pair.value, value);
	  return tvalue;
	}
	temp=temp->next;
      }

      temp=kos[shardId].hash[hash_pos].lst_item;  
      item->next=temp;
      kos[shardId].hash[hash_pos].lst_item = item;

    return NULL;
}

char* shard_remove( int shardId, char* key) {
	char* tvalue=(char*)malloc(sizeof(char)*20);
	int hash_pos=hash(key);
	LstItem *temp, *actual;
	
	if(kos[shardId].hash[hash_pos].lst_item==NULL){
	  return NULL;
	  
	}
	
	actual=kos[shardId].hash[hash_pos].lst_item; 
	if(strcmp(key, actual->pair.key) ==0){
	   strcpy(tvalue, actual->pair.value); 
	   kos[shardId].hash[hash_pos].lst_item = actual->next; 
	   free(actual); 
	   return tvalue; 
	  
	} 
	while(actual->next != NULL){ 
	  if(strcmp(key, actual->next->pair.key) == 0){ 
	    strcpy(tvalue, actual->next->pair.value);
	    temp=actual->next;
	    actual->next=temp->next;
	    free(temp);
	    return tvalue;
	    
	  }
	  actual=actual->next;
	  
	} 

	return NULL;
}

KV_t* shard_getAllKeys(int shardId, int dim) {
	  KV_t* allkeys=(KV_t*)malloc(sizeof(KV_t)*dim);
	  LstItem* act;
	  int i=0,j=0;
	  if(dim<0)     {
	                printf("ERROR: kos_getAllKeys: invalid value for dim\n");
	                return NULL;}

	  while(i<HT_SIZE){
	  if(kos[shardId].hash[i].lst_item!=NULL){

	      act=kos[shardId].hash[i].lst_item;
	      while(act!=NULL){
	          if(dim>0){
	        strcpy(allkeys[j].key, act->pair.key);

	        strcpy(allkeys[j].value, act->pair.value);


	        j++;
	        dim--;
	        act=act->next;

	      }
	        else return allkeys;
	      }}
	    i++;
	  }
	        if(dim>0){
	                printf("ERROR: kos_getAllKeys: not enough elements in shard %d\n", shardId);
	                return NULL;}
	                return NULL;
	}
