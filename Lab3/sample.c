#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#define PRECISION 0.000001
#define RANGESIZE 5
#define DATA 0
#define RESULT 1
#define FINISH 2
#define TWINOFFSET 2
#define INITIAL_ARRAY_SIZE 1000000
#define MAX_LINE_LENGTH 16

//#define DEBUG

unsigned long* read_csv_to_int_array(char* filename, int* count);
void mergeSort(unsigned long arr[], int l, int r);

int
f (unsigned long x)
{
	if(x<2 || x%2==0)
		return 0;
	if(x==2||x==3)
		return 1;
	
    for(unsigned long i=sqrt(x); i>1;i--){
		if(x%i==0)
			return 0;
	}
	return 1;
}




int
SimpleIntegration (int a, int b,unsigned long* arr)
{
    
	int i=a;
    int sum = 0;
	int offset=0;

	int potentialTwin=0;
    while (i<=b)
	{
		//printf("%ld",i);
		if(potentialTwin){
			if(!f(arr[i])){
				if(offset>=TWINOFFSET){
					offset=0;
					potentialTwin=0;
				}else
					offset++;
			}else{
				if(offset==TWINOFFSET-1)
					sum++;
				offset=0;
				
			}
		}

		if(!potentialTwin&&f(arr[i]))
			potentialTwin=1;
			
		i++;
		
		
	}
	
    return sum;
}

int
main (int argc, char **argv)
{
    MPI_Request *requests;
    int requestcount = 0;
    int requestcompleted;
    int myrank, proccount;
	int count=0;
	unsigned long* array = read_csv_to_int_array(argv[1], &count);
	int arraySize = count;
	mergeSort(array, 0, arraySize - 1);
    double a = 0, b = arraySize - 1;
    double *ranges;
    double range[2];
    double result = 0;
    double *resulttemp;
    int sentcount = 0;
    int recvcount = 0;
    int i;
    MPI_Status status;

    // Initialize MPI                                                                                                                                     
    MPI_Init (&argc, &argv);

    // find out my rank                                                                                                                                
    MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

    // find out the number of processes in MPI_COMM_WORLD                                                           
    MPI_Comm_size (MPI_COMM_WORLD, &proccount);

    if (proccount < 2)
    {
        printf ("Run with at least 2 processes");
	MPI_Finalize ();
	return -1;
    }

    if (((b - a) / RANGESIZE) < 2 * (proccount - 1))
    {
        printf ("More subranges needed");
	MPI_Finalize ();
	return -1;
    }

    // now the master will distribute the data and slave processes will perform computations
    if (myrank == 0)
    {
        requests = (MPI_Request *) malloc (3 * (proccount - 1) *
			sizeof (MPI_Request));

	if (!requests)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	ranges = (double *) malloc (4 * (proccount - 1) * sizeof (double));
	if (!ranges)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	resulttemp = (double *) malloc ((proccount - 1) * sizeof (double));
	if (!resulttemp)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	range[0] = a;

	// first distribute some ranges to all slaves
	for (i = 1; i < proccount; i++)
	{
            range[1] = range[0] + RANGESIZE;
#ifdef DEBUG
	    printf ("\nMaster sending range %f,%f to process %d",
    			    range[0], range[1], i);
	    fflush (stdout);
#endif
	    // send it to process i
	    MPI_Send (range, 2, MPI_DOUBLE, i, DATA, MPI_COMM_WORLD);
	    sentcount++;
	    range[0] = range[1];
	}

	// the first proccount requests will be for receiving, the latter ones for sending
	for (i = 0; i < 2 * (proccount - 1); i++)
  		requests[i] = MPI_REQUEST_NULL;	// none active at this point

	// start receiving for results from the slaves
	for (i = 1; i < proccount; i++)
            MPI_Irecv (&(resulttemp[i - 1]), 1, MPI_DOUBLE, i, RESULT,
   			    MPI_COMM_WORLD, &(requests[i - 1]));

	// start sending new data parts to the slaves
	for (i = 1; i < proccount; i++)
	{
            range[1] = range[0] + RANGESIZE;
#ifdef DEBUG
	    printf ("\nMaster sending range %f,%f to process %d",
    			    range[0], range[1], i);
	    fflush (stdout);
#endif
	    ranges[2 * i - 2] = range[0];
	    ranges[2 * i - 1] = range[1];

	    // send it to process i
	    MPI_Isend (&(ranges[2 * i - 2]), 2, MPI_DOUBLE, i, DATA,
 			    MPI_COMM_WORLD, &(requests[proccount - 2 + i]));

	    sentcount++;
	    range[0] = range[1];
	}
	while (range[1] < b)
	{
#ifdef DEBUG
            printf ("\nMaster waiting for completion of requests");
	    fflush (stdout);
#endif
	    // wait for completion of any of the requests
	    MPI_Waitany (2 * proccount - 2, requests, &requestcompleted,
			    MPI_STATUS_IGNORE);
	    
	    // if it is a result then send new data to the process
	    // and add the result
	    if (requestcompleted < (proccount - 1))
	    {
                result += resulttemp[requestcompleted];
		recvcount++;
#ifdef DEBUG
		printf ("\nMaster received %d result %f from process %d", recvcount,
				resulttemp[requestcompleted], requestcompleted + 1);
		fflush (stdout);
#endif
                // first check if the send has terminated
		MPI_Wait (&(requests[proccount - 1 + requestcompleted]),
				MPI_STATUS_IGNORE);

                // now send some new data portion to this process
		range[1] = range[0] + RANGESIZE;

		if (range[1] > b)
			range[1] = b;
#ifdef DEBUG
		printf ("\nMaster sending range %f,%f to process %d",
				range[0], range[1], requestcompleted + 1);
		fflush (stdout);
#endif
		ranges[2 * requestcompleted] = range[0];
		ranges[2 * requestcompleted + 1] = range[1];
		MPI_Isend (&(ranges[2 * requestcompleted]), 2, MPI_DOUBLE,
				requestcompleted + 1, DATA, MPI_COMM_WORLD,
				&(requests[proccount - 1 + requestcompleted]));
		sentcount++;
		range[0] = range[1];

                // now issue a corresponding recv
		MPI_Irecv (&(resulttemp[requestcompleted]), 1,
				MPI_DOUBLE, requestcompleted + 1, RESULT,
				MPI_COMM_WORLD,
				&(requests[requestcompleted]));
	    }
	}
	// now send the FINISHING ranges to the slaves
	// shut down the slaves
	range[0] = range[1];
	for (i = 1; i < proccount; i++)
	{
#ifdef DEBUG
            printf("\nMaster sending FINISHING range %f,%f to process %d",
       			    range[0], range[1], i);
	    fflush (stdout);
#endif
	    ranges[2 * i - 4 + 2 * proccount] = range[0];
	    ranges[2 * i - 3 + 2 * proccount] = range[1];
	    MPI_Isend (range, 2, MPI_DOUBLE, i, DATA, MPI_COMM_WORLD,
 			    &(requests[2 * proccount - 3 + i]));
	}
#ifdef DEBUG
	printf ("\nMaster before MPI_Waitall with total proccount=%d",
      			proccount);
	fflush (stdout);
#endif
	// now receive results from the processes - that is finalize the pending requests
        MPI_Waitall (3 * proccount - 3, requests, MPI_STATUSES_IGNORE);
#ifdef DEBUG
	printf ("\nMaster after MPI_Waitall with total proccount=%d",
      			proccount);
	fflush (stdout);
#endif
	// now simply add the results
	for (i = 0; i < (proccount - 1); i++)
	{
            result += resulttemp[i];
	}
	// now receive results for the initial sends
        for (i = 0; i < (proccount - 1); i++)
	{
#ifdef DEBUG
            printf ("\nMaster receiving result from process %d", i + 1);
	    fflush (stdout);
#endif
	    MPI_Recv (&(resulttemp[i]), 1, MPI_DOUBLE, i + 1, RESULT,
  			    MPI_COMM_WORLD, &status);
	    result += resulttemp[i];
	    recvcount++;
#ifdef DEBUG
	    printf ("\nMaster received %d result %f from process %d",
    			    recvcount, resulttemp[i], i + 1);
	    fflush (stdout);
#endif
	}
	// now display the result
	printf ("\nHi, I am process 0, the result is %f\n", result);
    }
    else //slave
    {		
        requests = (MPI_Request *) malloc (2 * sizeof (MPI_Request));

	if (!requests)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	requests[0] = requests[1] = MPI_REQUEST_NULL;
	ranges = (double *) malloc (2 * sizeof (double));

	if (!ranges)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	resulttemp = (double *) malloc (2 * sizeof (double));

	if (!resulttemp)
	{
            printf ("\nNot enough memory");
	    MPI_Finalize ();
	    return -1;
	}

	// first receive the initial data
        MPI_Recv (range, 2, MPI_DOUBLE, 0, DATA, MPI_COMM_WORLD, &status);
#ifdef DEBUG
	printf ("\nSlave received range %f,%f", range[0], range[1]);
	fflush (stdout);
#endif
        while (range[0] < range[1])
	{			
            // if there is some data to process
	    // before computing the next part start receiving a new data part
	    MPI_Irecv (ranges, 2, MPI_DOUBLE, 0, DATA, MPI_COMM_WORLD,
 			    &(requests[0]));

	    // compute my part
            resulttemp[1] = SimpleIntegration (range[0], range[1], array);
#ifdef DEBUG
	    printf ("\nSlave just computed range %f,%f", range[0],
    			    range[1]);
	    fflush (stdout);
#endif
            // now finish receiving the new part
	    // and finish sending the previous results back to the master
            MPI_Waitall (2, requests, MPI_STATUSES_IGNORE);
#ifdef DEBUG
	    printf ("\nSlave just received range %f,%f", ranges[0],
    			    ranges[1]);
	    fflush (stdout);
#endif
	    range[0] = ranges[0];
	    range[1] = ranges[1];
	    resulttemp[0] = resulttemp[1];

	    // and start sending the results back
            MPI_Isend (&resulttemp[0], 1, MPI_DOUBLE, 0, RESULT,
 			    MPI_COMM_WORLD, &(requests[1]));
#ifdef DEBUG
	    printf("\nSlave just initiated send to master with result %f",
       			    resulttemp[0]);
	    fflush (stdout);
#endif
	}

	// now finish sending the last results to the master
	 MPI_Wait (&(requests[1]), MPI_STATUS_IGNORE);
    }

    // Shut down MPI                                                                                                                                  
    MPI_Finalize ();

#ifdef DEBUG
    printf ("\nProcess %d finished", myrank);
    fflush (stdout);
#endif

    return 0;
	free(array);
}

unsigned long* read_csv_to_int_array(char* filename, int* count) {
    FILE* file = fopen(filename, "r"); // Open the file in read mode
    if (file == NULL) {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }

    // Dynamic array setup
    int capacity = INITIAL_ARRAY_SIZE;
    unsigned long* numbers = (unsigned long*)malloc(capacity * sizeof(unsigned long));
    if (numbers == NULL) {
        perror("Memory allocation failed");
        fclose(file);
        exit(EXIT_FAILURE);
    }
    *count = 0;

    char line[MAX_LINE_LENGTH];
    while (fgets(line, MAX_LINE_LENGTH, file) != NULL) { // Read each line
        // Remove potential newline character
        line[strcspn(line, "\n")] = 0;

		// Convert string token to integer
		unsigned long num = atoll(line);

		// Check if array needs resizing
		if (*count >= capacity) {
			capacity *= 2;
			unsigned long* temp = (unsigned long*)realloc(numbers, capacity * sizeof(unsigned long));
			if (temp == NULL) {
				perror("Memory re-allocation failed");
				free(numbers);
				fclose(file);
				exit(EXIT_FAILURE);
			}
			numbers = temp;
		}

		numbers[*count] = num;
		(*count)++;
    }

    fclose(file); // Close the file
    return numbers;
}

void merge(unsigned long arr[], int l, int m, int r){
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;

    // Create temp arrays
    unsigned long* L = (unsigned long*)malloc(n1 * sizeof(unsigned long));
    unsigned long* R = (unsigned long*)malloc(n2 * sizeof(unsigned long));

    // Copy data to temp arrays L[] and R[]
    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];

    // Merge the temp arrays back into arr[l..r]
    i = 0;
    j = 0;
    k = l;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
            i++;
        }
        else {
            arr[k] = R[j];
            j++;
        }
        k++;
    }

    // Copy the remaining elements of L[]
    while (i < n1) {
        arr[k] = L[i];
        i++;
        k++;
    }

    // Copy the remaining elements of R[]
    while (j < n2) {
        arr[k] = R[j];
        j++;
        k++;
    }

    free(L);
    free(R);
}

void mergeSort(unsigned long arr[], int l, int r){
    if (l < r) {
        int m = l + (r - l) / 2;

        // Sort first and second halves
        mergeSort(arr, l, m);
        mergeSort(arr, m + 1, r);

        merge(arr, l, m, r);
    }
}
