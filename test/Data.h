
#include	"../src/DB/Data.h"
#include	"../src/Log.h"

namespace MonoDB {

	Bool DataTestHandler() {
		Bool result = true;
		Log l("test/log/Data.log", L_ALL);

		const Uint64 number = 100000;
		Data* d = new Data("test/temp/Data/", 2 * GB);
		clock_t b, e;

		std::map<Uint64, Uint64> checker;
		b = clock();
		for(Uint32 i = 0; i < number; i++) {
			Uint64 value = (i + 1) * 2 + 3;
			Uint64 loc = d->Write((Byte*)&value, sizeof(Uint64));
			if(loc != NotFound) checker.insert(std::pair<Uint64, Uint64>(loc, value));
			else {
				result = false;
				l.Show("[ERROR] data write: %u (%llu) \n", i, value);
			}
		}
		e = clock();
		l.Show("[REPORT] data write %d (%lf sec) \n", number, (Float)(e - b) / 1000000);
		
		b = clock();
		for(std::map<Uint64, Uint64>::iterator i = checker.begin(); i != checker.end(); i++) {
			Uint64* value = NULL;
			Uint32 size = d->Read(i->first, (Byte**)&value);
			if(size == 0) {
				result = false;
				l.Show("[ERROR] data read: not found %u \n", i);
			}
			else if(*value != i->second) {
				result = false;
				l.Show("[ERROR] data read: value not match (%llu vs %llu) \n", *value, i->second);
			}
			CLEAR(value);
		}
		e = clock();
		l.Show("[REPORT] data read %d (%lf sec) \n", number, (Float)(e - b) / 1000000);

		FREE(d);
		return result;
	}

}
