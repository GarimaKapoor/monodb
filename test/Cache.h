
#include	"../src/Cache.h"
#include	"../src/Log.h"

namespace MonoDB {

	Bool CacheTestHandler() {
		Bool result = true;
		Log l("test/log/Cache.log", L_ALL);

		const Int32	number = 10000;
		const Int32	memorySize = 64 * sizeof(Uint64);

		Cache<Uint64, Uint64>* c = new Cache<Uint64, Uint64>(memorySize);
		Time b, e;

		/// test cache add
		gettimeofday(&b, NULL);
		for(Uint64 i = 0; i < number; i++) {
			Uint64 key = (i + 1);
			Uint64 value = (i + 1) * 100;
			if(!c->Add(key, value, sizeof(Uint64))) {
				result = false;
				l.Show("[ERROR] cache add: key %llu \n", key);
			}
		}
		gettimeofday(&e, NULL);
		l.Show("[REPORT] cache add %d (%u sec) \n", number, (e.tv_sec - b.tv_sec));
		
		/// test cache get
		gettimeofday(&b, NULL);
		for(Uint64 i = 0; i < number; i++) {
			Uint64 key = (i + 1);
			Uint64 value = (i + 1) * 100;

			Uint64 output = 0;
			if(c->Get(key, &output)) {
				if(output == value)
					l.Show("[REPORT] cache hit %llu (%llu vs %llu) \n", key, value, output);
			}
		}
		gettimeofday(&e, NULL);
		l.Show("[REPORT] cache get %d (%u sec) \n", number, (e.tv_sec - b.tv_sec));

		FREE(c);
		return result;
	}

}
