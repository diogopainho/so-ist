#include <delay.h>
#include <unistd.h>

#define DELAY 0
#define DELAY_TIME 2000

void delay() {
	
	#ifdef DELAY
		usleep(DELAY_TIME);
	#endif
}
