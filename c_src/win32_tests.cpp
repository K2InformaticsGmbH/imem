#include <stdio.h>

#include "Windows.h"

int main(int argc, char *argv[])
{
    LARGE_INTEGER lpPerformanceCount;
    QueryPerformanceCounter(&lpPerformanceCount);
    printf("QueryPerformanceCounter %llu\n", lpPerformanceCount.QuadPart);
    QueryPerformanceCounter(&lpPerformanceCount);
    printf("QueryPerformanceCounter %llu\n", lpPerformanceCount.QuadPart);

    printf("sizeof(LARGE_INTEGER) %u, sizeof(unsigned long) %u, sizeof(unsigned long long) %u, sizeof(WORD) %u, sizeof(DWORD) %u\n",
     sizeof(LARGE_INTEGER), sizeof(unsigned long), sizeof(unsigned long long), sizeof(WORD), sizeof(DWORD));

printf("WINVER %u, _WIN32_WINNT_WIN7 %u\n", WINVER, _WIN32_WINNT_WIN7);
#if defined(_WIN32_WINNT_WIN8) && WINVER > _WIN32_WINNT_WIN8
    FILETIME ft;
    GetSystemTimePreciseAsFileTime(&ft);

    printf("FILETIME.dwHighDateTime %x, FILETIME.dwLowDateTime %x\n",
     ft.dwHighDateTime, ft.dwLowDateTime);
#endif

    return 0;
}
