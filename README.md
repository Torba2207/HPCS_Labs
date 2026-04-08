# HPCS_Labs
High performance computing systems Labs

## Prerequisites

You will need OpenMPI installed to compile and run programs from these labs.

## Compilation

Compile your MPI program using `mpicc`:

```bash
mpicc [compiler flags] -o output_name program_name.c
```

Common flags:
- `-lm` - Link math library (if using `<math.h>` functions like `sqrt()`)
- `-O2` - Optimization level
- Other flags as needed by your program

## Execution

Run the compiled executable with `mpirun`:

```bash
mpirun -n [number of processes] ./output_name [program arguments]
```

Example:
```bash
mpirun -n 4 ./output_name input.csv
```
