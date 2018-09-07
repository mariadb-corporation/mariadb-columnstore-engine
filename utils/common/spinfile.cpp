#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <climits>
#include <stdlib.h>

enum SpinState {
  SpinStop=0, // Stop the spin
  Spin=1,     // Spin
};

// Set the following variable to SpinStop(value 0) to stop spinning.

SpinState mcsspin = Spin;

// 
// mcs_spin - make the process spin if the file exists in /tmp
//
extern void mcs_spin (
    const char* filename ) // base filename to spin on
{
	bool done = false;
	char path[PATH_MAX+1];
	char cmd[PATH_MAX+80];
	
	sprintf(path, "/tmp/%s", filename);
	while ((mcsspin == Spin) && access(path, F_OK) == 0) 
	{
		if (!done) {
			done = true;
			snprintf(cmd, PATH_MAX+80, "echo `date` pid %d spinning on file %s >>/tmp/spinner\n", getpid(), path);
			chmod("/tmp/spinner",0777);  
			system(cmd);
		}
	    sleep(1);
	}
	
	// restore the spinner for next call
	if ( mcsspin == SpinStop )
	    mcsspin = Spin;
}

extern bool mcs_would_spin (
    const char* filename ) // base filename to spin on
{
	bool done = false;
	char path[PATH_MAX+1];
	
	sprintf(path, "/tmp/%s", filename);
	return ((mcsspin == Spin) && (access(path, F_OK) == 0)); 
}
extern void debugme ()
{
  mcs_spin("spin_debugme");
}
