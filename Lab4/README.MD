# Lab 4 - Prime Twins with OpenMP

## Compilation

Compile the program using `gcc` with OpenMP:

```bash
gcc -fopenmp sample.c -o sample -lm
```

Flags:
- `-fopenmp` - Enable OpenMP for parallel processing
- `-o sample` - Output executable name
- `-lm` - Link math library

## Execution

Run the compiled executable with a CSV input file and number of threads:

```bash
./sample arr0.csv 4
```

Arguments:
- `arr0.csv` - Input CSV file containing numbers (one number per line)
- `4` - Number of threads to use for parallel processing

Example with different inputs:
```bash
./sample arr1.csv 8
./sample arr2.csv 2
```

The program counts prime twin pairs in the input data using parallel processing.
