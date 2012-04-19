#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
 
/* The argument now should be a double (not a pointer to a double) */
#define GET_TIME(now) { \
struct timeval t; \
gettimeofday(&t, NULL); \
  now = t.tv_sec + t.tv_usec/1000000.0; \
}
 


// getLine.c                                      Stan Eisenstat (10/27/09)

// Read a line of text using the file pointer *fp and returns a pointer to a
// null-terminated string that contains the text read, include the newline (if
// any) that ends the line.  Storage for the line is allocated with malloc()
// and realloc().  If the end of the file is reached before any characters are
// read, then the NULL pointer is returned.

char *getLine(FILE *fp)
{
	char *line;                 // Line being read
  int size;                   // #chars allocated
  int c, i;

  size = sizeof(double);                      // Minimum allocation
  line = malloc (size);
  for (i = 0;  (c = getc(fp)) != EOF; )  {
	if (i == size-1) {
	    size *= 2;                          // Double allocation
	    line = realloc (line, size);
	}
	line[i++] = c;
	if (c == '\n')                          // Break on newline
	    break;
  }

  if (c == EOF && i == 0)  {                  // Check for immediate EOF
	free (line);
	return NULL;
  }

  line[i++] = '\0';                           // Terminate line
  line = realloc (line, i);                   // Trim excess storage

  return (line);
}

double second_(void) {
	double now;
	GET_TIME(now);
	return (now);
}

int main(int argc, char**argv) {
	
	FILE *f = fopen(argv[1], "r");

	char** strings = malloc(sizeof(*strings) * 1000);
	int size = 1000;
	int count = 0;
	while( (strings[count] = getLine(f))) {
		count++;
		if(count == size) {
			strings = realloc(strings, sizeof(*strings) * (size*=2));
		}
	}
	
	double time = second_();
	char ** strings2 = malloc (sizeof (*strings) * (count-1));
	memmove(strings2, strings, count-1);
	printf("%lf\n", (second_()-time));
}
