
#include	"../src/Smarttest.h"
#include	"Convert.h"
#include	"Hashmap.h"
#include	"Cache.h"
#include	"Network.h"
#include	"Data.h"
#include	"Btree.h"

using namespace MonoDB;

int main(int argc, char** argv) {
	Smarttest test;
	test.Add("Convert", ConvertTestHandler);
	test.Add("Hashmap", HashmapTestHandler);
	test.Add("Cache", CacheTestHandler);
	test.Add("Network", NetworkTestHandler);
	test.Add("DB/Data", DataTestHandler);
	test.Add("DB/Btree", BtreeTestHandler);
	test.Execute();
	return 0;
}
