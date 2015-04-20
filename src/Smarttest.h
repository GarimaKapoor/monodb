
#ifndef _MONO_SMARTTEST_
#define _MONO_SMARTTEST_

#include "Define.h"
#include "Log.h"

namespace MonoDB {
	THREAD_UNSAFE class Smarttest {
		private:
			typedef	Bool (*SmarttestHandler)();
			typedef	struct {
				std::string			name;
				SmarttestHandler	handler;
			} SmarttestItem;
			std::list<SmarttestItem> _queue;

		public:
			Smarttest() {}
			~Smarttest() {}

			void	Add(const Char* name, SmarttestHandler handler) {
				SmarttestItem item;
				item.name = name;
				item.handler = handler;
				_queue.push_back(item);
			}
			void	Execute() {
				Uint32	totalCount = 0;
				Uint32	successCount = 0;

				Log l;
				l.Show("start smart test: \n");
				for(std::list<SmarttestItem>::iterator i = _queue.begin();
					i != _queue.end(); i++) {
					SmarttestItem item = *i;
					totalCount++;
					l.Show("testing %s... ", item.name.c_str());
					if(item.handler()) {
						successCount++;
						l.Show("success \n");
					}
					else l.Show("fail \n");
				}
				l.Show("== smart test report: total %u success %u \n", totalCount, successCount);
			}
	};
}

#endif
