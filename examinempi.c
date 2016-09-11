#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <omp.h>
#include <mpi.h>

#define lowLimit 12
#define highLimit 30
#define STR_LEN 31
#define BUF_SIZE 5000000
#define TOK_LEN 9

int checkArgs(char **argv);
long read(FILE *data, char *buffer, long lines, long offset);
void parse(char *data, int nums, char returnBuffer[3][TOK_LEN + 1], long line);
int checkTime(double runTime,struct timespec start);
double getElapsedTime(struct timespec start, struct timespec end);

int main(int argc, char *argv[])
{
    struct timespec start, end;
    long file_size, lines, limit, readLines = 0, remains, offset, k;
    long sum=0, total_sum = 0, lineSum = 0, totalLines = 0;
    int rank, size, i, openmp_threads, openmpi_processes;
    double runTime, timePassed;
    char runTimeSet = 0, finalize = 0, done = 0;
    FILE *data;

    char usage[] = "Usage: examine\n\t[number of collisions ( -1 as many as in file)]\n\t[maximum run time (-1 unlimited time)]\n\t[input file]\n\t[num of openmp threads (-1 all available threads)]\n\t[num of openmpi processes (-1 all available processes)]\n";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(checkArgs(argv) == 1) {
        printf("%s",usage);
        return 1;
    }

    limit = atol(argv[1]);
    runTime = atof(argv[2]);
    data = fopen(argv[3],"r");
    openmp_threads = atoi(argv[4]);
    openmpi_processes = atoi(argv[5]);

    if(data == NULL) {
        printf("Could not open specified file.\n");
        return 1;
    }

    if(runTime!=-1.000000 && runTime >= 0) runTimeSet = 1; //runTimeSet indicates whether the user has specified a run time limit.

    if(openmp_threads == -1 || openmp_threads >= omp_get_max_threads()) omp_set_num_threads(omp_get_max_threads());
    else if(openmp_threads > 0) omp_set_num_threads(openmp_threads);
    
    MPI_Barrier(MPI_COMM_WORLD);
    clock_gettime(CLOCK_MONOTONIC,&start);

    /*
    Ranks contains the ranks of the proccesses that will be used in the actual processing.
    Size is set according to user's input.
    */
    int ranks[size];

    if(openmpi_processes<size && openmpi_processes>0)
    {
        size = openmpi_processes;
    }

    for(i=0; i<size; i++) ranks[i] = i;

    /*
    Create a communicator for the processes that do the actual processing
    Needed when processes specified by user are less than given ones
    */
    MPI_Group worldGroup;
    MPI_Comm_group(MPI_COMM_WORLD, &worldGroup);
    MPI_Group processGroup;
    MPI_Group_incl(worldGroup, size, ranks, &processGroup);

    MPI_Comm processComm;
    MPI_Comm_create(MPI_COMM_WORLD, processGroup, &processComm);
    /* Communicator made */

    for(i=0; i<size; i++) {
        if(rank == i) {
            //Find file size
            fseek(data,0,SEEK_END);
            file_size = ftell(data);

            //If limit is set, then file size is size until limit is met
            //Limit is given in lines, each line is STR_LEN bytes, so we fseek at limit*STR_LEN bytes
            if(limit != -1 && limit < file_size/STR_LEN) fseek(data,limit*STR_LEN,SEEK_SET);

            file_size = ftell(data);
            rewind(data);

            //Lines is the number of lines that each process will handle
            //Offset is used when reading, to move the file pointer to the desired line
            //Remains is the remainder of the division, they are added to the lines of the last process
            lines = (file_size/STR_LEN)/size;
            offset = lines*STR_LEN*i;
            remains = (file_size/STR_LEN)%size;

            if(rank == size-1) lines += remains;

            //Buffer that holds the read lines
            char *buffer = malloc(sizeof(char) * BUF_SIZE * STR_LEN);

            //Nums is an array that contains the 3 numbers of each line
            //A line is parsed and divided into 3 strings, stored in cords
            //TOK_LEN is the number of digits, +1 for \0
            double nums[3];
            char cords[3][TOK_LEN + 1];

            //First run time check
            //Checks if run time is more than given run time (if given), in which case, finalize is set to 1, so while won't start.
            if(runTimeSet) {
                if(checkTime(runTime,start) == 1)
                {
                    finalize = 1;
                }
            }
            while(!done && !finalize) {
                //The read function is used to get lines from our file
                //It stores the lines at buffer, and returns the number of read lines
                readLines = read(data,buffer,lines,offset);

                //Second run time check, before processing
                if(runTimeSet) {
                    if(checkTime(runTime,start) == 1)
                    {
                        finalize = 1;
                    }
                }

                if(!finalize) {
                    #pragma omp parallel for shared(buffer, start, finalize) private(cords, nums) reduction(+:sum,lineSum)
                    for(k=0; k<readLines; k++)
                    {
                        /*
                        Parses the kth line of the buffer and stores it in cords
                        Then we get doubles with atof, called for each number only if the previous one is inside the limits, as atof costs a lot
                        */
                        lineSum = lineSum + 1;
                        parse(buffer,3,cords,k);
                        nums[0] = atof(cords[0]);
                        if(nums[0]>=lowLimit && nums[0]<=highLimit) {
                            nums[1] = atof(cords[1]);
                            if(nums[1] >= lowLimit && nums[1] <= highLimit) {
                                nums[2] = atof(cords[2]);
                                if(nums[2] >= lowLimit && nums[2] <= highLimit) {
                                    sum = sum + 1;
                                }
                            }
                        }

                        //Third and last run time check, checks after every line is processed
                        if(runTimeSet) {
                            if(checkTime(runTime,start) == 1)
                            {
                                finalize = 1;
                                //Exits the loop
                                k = readLines;
                            }
                        }

                    }
                }

                //Update the offset, reduce the lines to be read, set done to 1 if all is read
                offset += readLines*STR_LEN;
                lines -= readLines;
                if(lines <= 0 || readLines == 0) done = 1;
            }

            //Sum up the read lines and the valid lines that are inside the limits
            MPI_Reduce (&sum, &total_sum, 1, MPI_LONG, MPI_SUM,0,processComm);
            MPI_Reduce (&lineSum, &totalLines, 1, MPI_LONG, MPI_SUM,0,processComm);
            free(buffer);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    clock_gettime(CLOCK_MONOTONIC,&end);

    //Only one process prints, rank 0
    if(rank == 0) {
        timePassed = getElapsedTime(start,end);
        if(finalize) printf("Given runtime surpassed.\n");
        printf("Total time: %f\n",timePassed);
        printf("Valid collisions: %ld\n",total_sum);
        printf("Total lines read: %ld\n",totalLines);
        printf("Lines read/sec: %f\n",totalLines/timePassed);
    }

    MPI_Finalize();
    return 0;
}

long read(FILE *data, char *buffer, long lines, long offset) {
    long result = 0;
    size_t length;
    fseek(data,offset,SEEK_SET);

    //We can read up to BUF_SIZE lines at once, each STR_LEN bytes, so length is lines*STR_LEN
    if(lines > BUF_SIZE) lines = BUF_SIZE;
    length = STR_LEN * lines;

    result = fread(buffer, sizeof(char), length, data);

    //Fread returns number of bytes read, so divided by STR_LEN we get number of lines read
    return result / STR_LEN;
}

void parse(char *data, int nums, char returnBuffer[3][TOK_LEN+1], long line) {
    int i,j, temp;

    //For i = 0 to TOK_LEN ( = 9), which is the number of digits
    //We already know the position of each digit
    //Line format is as such: "12.345678 12.345678 12.345678 \n"
    //First number is from 0 [ 0*(TOK_LEN+1) ] to 8
    //Second number is from 10 [ 1*(TOK_LEN+1) ] (9 is whitespace) to 18
    //Third number is from 20 [ 2*(TOK_LEN+1) ] (19 is whitespace) to 28
    //That is for first line, for each line we use the line's number * STR_LEN as an offset at temp

    for(i=0; i<TOK_LEN; i++) {
        temp = i + line*STR_LEN;
        for(j=0; j<nums; j++) {
            returnBuffer[j][i] = data[(temp + j*(TOK_LEN + 1))];
        }
    }

    //Add the string terminator
    for(j=0; j<nums; j++) returnBuffer[j][TOK_LEN] = '\0';
}

int checkTime(double runTime,struct timespec start) {
    //Returns 1 if we surpassed specified runtime, else 0

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    double totalTime = getElapsedTime(start, now);

    if(totalTime >=runTime) return 1;
    return 0;
}

double getElapsedTime(struct timespec start, struct timespec end) {
    //Returns elapsed time between two timespecs

    const int DAS_NANO_SECONDS_IN_SEC = 1000000000;
    long timeElapsed_s = end.tv_sec - start.tv_sec;
    long timeElapsed_n = end.tv_nsec - start.tv_nsec;
    if ( timeElapsed_n < 0 ) {
        timeElapsed_n =
            DAS_NANO_SECONDS_IN_SEC + timeElapsed_n;
        timeElapsed_s--;
    }

    double totalTime = timeElapsed_s + (timeElapsed_n/1000000000.0);
    return totalTime;
}

int checkArgs(char **argv) {
    //Checks if given args are valid

    if(argv[1] == NULL) {
        printf("Number of collisions not specified.\n");
        return 1;
    }

    long lines = atol(argv[1]);
    if(lines < 0 && lines != -1) {
        printf("Number of collisions cannot be negative.\n");
    }

    if(argv[2] == NULL) {
        printf("Maximum run time not specified.\n");
        return 1;
    }

    double runTime = atof(argv[2]);
    if(runTime < 0 && runTime != -1) {
        printf("Maximum run time cannot be negative.\n");
    }

    if(argv[3] == NULL) {
        printf("Input file not specified.\n");
        return 1;
    }

    if(argv[4] == NULL) {
        printf("OpenMP threads not specified.\n");
        return 1;
    }

    int threads = atoi(argv[4]);
    if(threads < 0 && threads != -1) {
        printf("Number of threads cannot be negative.\n");
    }

    if(argv[5] == NULL) {
        printf("OpenMPI process number not specified.\n");
        return 1;
    }

    int processes = atoi(argv[5]);
    if(processes < 0 && processes != -1) {
        printf("Number of processes cannot be negative.\n");
    }

    return 0;
}

