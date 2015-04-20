
#include	"../src/Hashmap.h"
#include	"../src/Log.h"

namespace MonoDB {

	Bool HashmapTestHandler() {
		Bool result = true;
		Log l("test/log/Hashmap.log", L_ALL);

		const Int32	number = 100000;
		const Int32	tableSize = 1024;

		Hashmap<Uint64, Uint64>* h = new Hashmap<Uint64, Uint64>(
			tableSize,
			Hashmap<Uint64, Uint64>::DefaultFunction
		);
		Time b, e;

		/// test hashmap set
		gettimeofday(&b, NULL);
		for(Uint64 i = 0; i < number; i++) {
			Uint64 key = (i + 1);
			Uint64 value = (i + 1) * 100;
			if(!h->Set(key, value)) {
				result = false;
				l.Show("[ERROR] h set: key %llu \n", key);
			}
		}
		gettimeofday(&e, NULL);
		l.Show("[REPORT] hashmap set %d (%u sec) \n", number, (e.tv_sec - b.tv_sec));
		
		/// test hashmap get
		gettimeofday(&b, NULL);
		for(Uint64 i = 0; i < number; i++) {
			Uint64 key = (i + 1);
			Uint64 output = 0;
			if(h->Get(key, &output)) {
				Uint64 value = (i + 1) * 100;
				if(output != value) {
					result = false;
					l.Show("[WARNING] hashmap get: key %llu value not match (%llu vs %llu) \n", key, output, value);
				}
			}
			else {
				result = false;
				l.Show("[ERROR] hashmap get: key %llu not found \n", key);
			}
		}
		gettimeofday(&e, NULL);
		l.Show("[REPORT] hashmap get %d (%u sec) \n", number, (e.tv_sec - b.tv_sec));
		l.Show("[REPORT] hashmap count %u \n", h->Count());

		FREE(h);
		l.Show("= HashmapTestHandler end \n");
		return result;
	}

}
