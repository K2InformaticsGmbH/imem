#include <stdio.h>

#include "Windows.h"

int main(int argc, char *argv[])
{
    LARGE_INTEGER lpPerformanceCount;
    QueryPerformanceCounter(&lpPerformanceCount);
    printf("QueryPerformanceCounter %llu\n", lpPerformanceCount.QuadPart);
    QueryPerformanceCounter(&lpPerformanceCount);
    printf("QueryPerformanceCounter %llu\n", lpPerformanceCount.QuadPart);

    printf("sizeof(LARGE_INTEGER) %u, sizeof(unsigned long) %u, sizeof(unsigned long long) %u\n",
     sizeof(LARGE_INTEGER), sizeof(unsigned long), sizeof(unsigned long long));

    return 0;
}