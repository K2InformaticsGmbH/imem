#include "stdafx.h"

extern void test(unsigned long long & megasecond, unsigned long long & second, unsigned long long & microsecond);

int main()
{
	unsigned long long megasecond, second, microsecond;
	test(megasecond, second, microsecond);
	printf("Mega %lld, Second %lld, Micro %lld\n", megasecond, second, microsecond);
    return 0;
}