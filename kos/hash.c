#include <unistd.h>
#include <hash.h>
 
int hash(char* key) {
     
    int i=0;
 
    if (key == NULL)
        return -1;
 
    while (*key != '\0') {
        i+=(int) *key;
        key++;
    }
 
    i=i % HT_SIZE;
 
    return i;
}

