#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <math.h>
#include <string.h>

#define INITIAL_ARRAY_SIZE 1000000
#define MAX_LINE_LENGTH 16



unsigned long* read_csv_to_int_array(char* filename, int* count);
void mergeSort(unsigned long arr[], int l, int r);
int removeDuplicates(unsigned long arr[], int size);

int isPrime(unsigned long x){
  if(x==2||x==3)
		return 1;
	if(x<2 || x%2==0)
		return 0;
	
	
  for(unsigned long i=3; i*i<=x;i+=2){
		if(x%i==0)
			return 0;
	}
	return 1;
}

/*
int countPrimeTwins(unsigned long* arr, int size, int* sum){
  int i=0;
    //int sum = 0;
    #pragma omp parallel for private(*arr) reduction(+:*sum) // reduction allow to execute operation on chosen variable in all threads
    for(i=0;i<size;i++){
        if (isPrime(arr[i])) {
            for (int j = i + 1; j < size && arr[j] <= arr[i] + 2; j++) {
                if (arr[j] == arr[i] + 2 && isPrime(arr[j])) {
                    *sum++;
                    break;
                }
            }
        }

        
    }
}
*/



int main(int argc,char **argv) {
  int sum=0;
  int count=0;
	unsigned long* array = read_csv_to_int_array(argv[1], &count);
  int threadnum=atoi(argv[2]); 
	int arraySize = count;
	mergeSort(array, 0, arraySize - 1);
	arraySize = removeDuplicates(array, arraySize);  // Remove duplicates after sorting

  //set number of threads
  omp_set_num_threads(threadnum);
  
  //lock object - usable for synchronization
  omp_lock_t writelock;

  omp_init_lock(&writelock);
  int i=0;
    //int sum = 0;
  

  #pragma omp parallel for reduction(+:sum)
  for(i = 0; i < arraySize; i++){
    if (isPrime(array[i])) {
        for (int j = i + 1; j < arraySize && array[j] <= array[i] + 2; j++) {
            if (array[j] == array[i] + 2 && isPrime(array[j])) {
                sum++;
                break;
            }
        }
    }
  }

  printf("Prime twins - %d\n", sum);
}

  /*
  int value=0;

  printf("Value at the begining is %d \n",value);


  //parallel - all thread execute below code in parallel
  #pragma omp parallel shared(value) 
  {
    //synchronization with atomic operation
    #pragma omp atomic update
    value++;
  }

 printf("Value after parallel (4 thread) is %d \n",value);

//parallel -  all thread execute below code in parallel - synchronization with lock 
#pragma omp parallel shared(value) 
{
  omp_set_lock(&writelock);
  value++;
  omp_unset_lock(&writelock);
}

 printf("Value after parallel (4 thread) is %d \n",value);

//parallel - all thread execute below code in parallel but critical section will be executed only by one thread at the same time
 #pragma omp parallel shared(value) 
{
  #pragma omp critical
  {
    value++;
  }
}

  long orders = 1000000000;
  long arraySize=0;
  long i=0;
  double beer;
  double total_beers = 0.0;

  //parallel for loop - all threads execute in parallel different parts of iterations
  #pragma omp parallel for private(beer) reduction(+:total_beers) // reduction allow to execute operation on chosen variable in all threads
  for(i=0;i<orders;i++) {
    beer = 1.0/pow((double)2,(double)i);
    total_beers = total_beers + beer;
  }



  printf("Your order is %f beers",total_beers);
  

}*/





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

int removeDuplicates(unsigned long arr[], int size) {
    if (size <= 1)
        return size;

    int j = 0;  // Index for unique elements
    for (int i = 1; i < size; i++) {
        if (arr[i] != arr[j]) {
            j++;
            arr[j] = arr[i];
        }
    }
    return j + 1;  // New size without duplicates
}