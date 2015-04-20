
#include	"../src/Convert.h"
#include	"../src/Log.h"

namespace MonoDB {

	Bool ConvertTestHandler() {
		Log l("test/log/Convert.log", L_ALL);
		const Int32 ConverttestIntCase = 6;
		const Char* ConverttestInt[ConverttestIntCase] = {
			"", "409", "-34", "-582a", "38d7f", "ead983"
		};
		const Int32 ConverttestUintCase = 6;
		const Char* ConverttestUint[ConverttestUintCase] = {
			"", "2098", "1209dfg0e", "24K", "-239", "af123"
		};
		const Int32 ConverttestFloatCase = 9;
		const Char* ConverttestFloat[ConverttestFloatCase] = {
			"", "10.2345", "126", "0.43595", ".239", "-2.8462", "1.as", "14d.39", "a9.22"
		};
		const Int32 ConverttestStringFromIntCase = 3;
		const Int64 ConverttestStringFromInt[ConverttestStringFromIntCase] = {
			0, 167, -316
		};
		const Int32 ConverttestStringFromUintCase = 3;
		const Uint64 ConverttestStringFromUint[ConverttestStringFromUintCase] = {
			0, 240, 409851234634L
		};
		const Int32 ConverttestStringFromFloatCase = 4;
		const Float ConverttestStringFromFloat[ConverttestStringFromFloatCase] = {
			0, 37.5093, .781259, -2.216
		};

		Bool result = true;
		l.Show("= ConverttestHandler start \n");
		
		for(Int32 i = 0; i < ConverttestIntCase; i++) {
			l.Show("[REPORT] convert toInt: %s vs %lld \n",
					ConverttestInt[i], Convert::toInt(ConverttestInt[i]));
		}
		for(Int32 i = 0; i < ConverttestUintCase; i++) {
			l.Show("[REPORT] convert toUint: %s vs %llu \n",
					ConverttestUint[i], Convert::toUint(ConverttestUint[i]));
		}
		for(Int32 i = 0; i < ConverttestFloatCase; i++) {
			l.Show("[REPORT] convert toFloat: %s vs %lf \n",
					ConverttestFloat[i], Convert::toFloat(ConverttestFloat[i]));
		}
		for(Int32 i = 0; i < ConverttestStringFromIntCase; i++) {
			l.Show("[REPORT] convert toString: %lld vs %s \n",
					ConverttestStringFromInt[i], Convert::toString(ConverttestStringFromInt[i]).c_str());
		}
		for(Int32 i = 0; i < ConverttestStringFromUintCase; i++) {
			l.Show("[REPORT] convert toString: %llu vs %s \n",
					ConverttestStringFromUint[i], Convert::toString(ConverttestStringFromUint[i]).c_str());
		}
		for(Int32 i = 0; i < ConverttestStringFromFloatCase; i++) {
			l.Show("[REPORT] convert toString: %lf vs %s \n",
					ConverttestStringFromFloat[i], Convert::toString(ConverttestStringFromFloat[i]).c_str());
		}		
		l.Show("= ConverttestHandler end \n");
		return result;
	}

}
