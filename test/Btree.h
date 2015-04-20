
#include	"../src/DB/Btree.h"
#include	"../src/Log.h"

namespace MonoDB {

	Int32 BtreeCompare(Byte* lb, Byte* rb, void* arg) {
		Int64 lv, rv;
		memcpy(&lv, lb, sizeof(Uint64));
		memcpy(&rv, rb, sizeof(Uint64));
		return (Int32)(lv - rv);
	}

	Bool BtreeTestHandler() {
		Bool result = true;
		Log l("test/log/Btree.log", L_ALL);

		const Uint64 number = 100000;
		Btree* bt = new Btree("test/temp/Btree/", sizeof(Uint64), sizeof(Uint64), 2 * GB);
		bt->SetCompare(BtreeCompare, NULL);
		clock_t b, e;

		/// Btree insert
		Byte* item = bt->CreateItem();
		b = clock();
		for(Uint32 i = 0; i < number; i++) {
			Uint64 key = i + 1;
			Uint64 value = (i + 1) * 2 + 3;

			Block stream(item, bt->KeySize() + bt->ValueSize());
			stream.Write(&key, sizeof(Uint64));
			stream.Write(&value, sizeof(Uint64));

			if(!bt->Insert(item)) {
				result = false;
				l.Show("[ERROR] btree write: %llu (%llu) \n", key, value);
			}
		}
		e = clock();
		l.Show("[REPORT] btree write %d (%lf sec) \n", number, (Float)(e - b) / 1000000);
		
		/// Btree get
		b = clock();
		for(Uint32 i = 0; i < number; i++) {
			Uint64 key = i + 1;
			Uint64 value = (i + 1) * 2 + 3;
			Uint64 output = 0;

			if(bt->Get((Byte*)&key, (Byte*)&output)) {
				if(value != output) {
					result = false;
					l.Show("[ERROR] btree read: value not match %llu (%llu vs %llu) \n", key, value, output);
				}
			}
			else {
				result = false;
				l.Show("[ERROR] btree read: %llu (%llu) \n", key, value);
			}
		}
		e = clock();
		l.Show("[REPORT] btree read %d (%lf sec) \n", number, (Float)(e - b) / 1000000);

		/// Btree remove
		b = clock();
		for(Uint32 i = (Uint32)(number / 2); i < number; i++) {
			Uint64 key = i + 1;
			if(!bt->Remove((Byte*)&key)) {
				result = false;
				l.Show("[ERROR] btree remove: %llu \n", key);
			}
		}
		e = clock();
		l.Show("[REPORT] btree remove %d (%lf sec) \n", (Int32)(number / 2), (Float)(e - b) / 1000000);

		/// Btree iterate
		b = clock();
		for(Btree::Iterator iter = bt->Begin(); iter.IsFound(); iter++) {
			Uint64 key = 0;
			Uint64 value = 0;
			if(Byte* item = iter.Get()) {
				Block stream(item, sizeof(Uint64) * 2);
				stream.Read(&key, sizeof(Uint64));
				stream.Read(&value, sizeof(Uint64));

				l.Show("[LOG] key %llu value %llu \n", key ,value);
			}
			else {
				result = false;
				l.Show("[ERROR] btree iterate \n");
			}
		}
		e = clock();
		l.Show("[REPORT] btree iterate %d (%lf sec) \n", (Int32)(number / 2), (Float)(e - b) / 1000000);

		CLEAR(item);
		FREE(bt);
		return result;
	}

}
