/*
 * File:   main.c
 * Author: aristeidis
 *
 * Created on March 5, 2016, 2:29 PM
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int checkArgs(char **argv);

int main(int argc, char** argv) {
    char usage[] = "Usage: \n\t[File name to save the coordinates]\n\t[Number of coordinates that will be used]\n";

    if(checkArgs(argv) == 1) {
        printf("%s",usage);
        return 1;
    }

    FILE *f = fopen(argv[1], "w");
    if (f == NULL) {
        printf("Error opening file!\n");
        exit(1);
    }

    int utime;
    long int ltime;
    int i,j;
    char zero[3];
    int coordNum = atol(argv[2]);

    float **cords;
    cords = malloc(coordNum*sizeof(float *));
    if(cords == NULL) {
        printf("Memory allocation error. Memory requested: %lu",sizeof(float *)*coordNum);
    }

    ltime = time(NULL);
    utime = (unsigned int) ltime / 2;
    srand(utime);

    for (i = 0; i < coordNum; i++) {
        cords[i] = malloc(3*sizeof(float));
        
        for(j=0; j<3; j++) zero[j] = '\0';

        cords[i][0] = (float) 34 * rand() / (RAND_MAX - 1);
        cords[i][1] = (float) 34 * rand() / (RAND_MAX - 1);
        cords[i][2] = (float) 34 * rand() / (RAND_MAX - 1);
        
        if(cords[i][0] < 10) zero[0] = '0';
        if(cords[i][1] < 10) zero[1] = '0';
        if(cords[i][2] < 10) zero[2] = '0';
        
        for(j=0; j<3; j++) {
            if(zero[j] != '\0') fprintf(f,"%c",zero[j]);
            fprintf(f,"%f ",cords[i][j]);
        }
        fprintf(f,"\n");

        free(cords[i]);
    }

    fclose(f);

    return 0;

}
int checkArgs(char **argv) {
    if(argv[1] == NULL) {
        printf("File name to save the coordinates not specified.\n");
        return 1;
    }

    if(argv[2] == NULL) {
        printf("Number of coordinates that will be used not specified.\n");
        return 1;
    }

    return 0;
}
