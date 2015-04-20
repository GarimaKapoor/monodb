
#ifndef _MONO_CONVERT_
#define _MONO_CONVERT_

#include "Define.h"
#include "Charcode.h"

namespace MonoDB {
	class Convert {
		public:
			/// string to number (int, uint, float)
			static Int64 toInt(const Char* s) {
				if(!s) return 0;
				Int64 result = 0;
				Int32 radix = 0;
				Bool negative = (s[0] == CC_SUB);
				if(negative) radix++;

				while(Char c = s[radix++]) {
					Int32 n = c - CC_NUMBER_0;
					if(0 <= n && n <= 9) result = result * 10 + n;
					else break;
				}
				if(negative) result = result * (-1);
				return result;
			}
			static Uint64 toUint(const Char* s) {
				if(!s) return 0;
				Uint64 result = 0;
				Int32 radix = 0;
				while(Char c = s[radix++]) {
					Int32 n = c - CC_NUMBER_0;
					if(0 <= n && n <= 9) result = result * 10 + n;
					else break;
				}
				return result;
			}
			static Float toFloat(const Char* s) {
				if(!s) return 0;
				Float result = 0;
				Int32 radix = 0;
				Int32 dot = 0;
				Bool negative = (s[0] == CC_SUB);
				if(negative) radix++;

				while(Char c = s[radix++]) {
					if(c == CC_DOT) {
						if(dot) break;
						dot = 1;
						continue;
					}
					Int32 n = c - CC_NUMBER_0;
					if(0 <= n && n <= 9)
						result = !dot ? (result * 10) + n : result + (Float)(n / pow(10, dot++));
					else break;
				}
				if(negative) result = result * (-1);
				return result;
			}

			/// number to string
			static std::string toString(const Int64 i) {
				std::string result = "";
				Int64 a = abs(i);
				while(a > 0) {
					Char c = (a % 10) + CC_NUMBER_0;
					a = (Int64)(a / 10);
					result = c + result;
				}
				if(i < 0) result = "-" + result;
				if(result == "") result = "0";
				return result;
			}
			static std::string toString(const Uint64 u) {
				std::string result = "";
				Uint64 a = u;
				while(a > 0) {
					Char c = (a % 10) + CC_NUMBER_0;
					a = (Uint64)(a / 10);
					result = c + result;
				}
				if(result == "") result = "0";
				return result;
			}
			static std::string toString(const Float f) {
				return toString(f, 8);
			}
			static std::string toString(const Float f, Uint32 p) {
				std::string result = "";
				Float a = f >= 0 ? f : f * (-1);
				Uint64 n = (Uint64)a;
				Float d = a - n;
				while(n > 0) {
					Char c = (n % 10) + CC_NUMBER_0;
					n = (Int64)(n / 10);
					result = c + result;
				}
				if(result == "") result = "0";
				if(f < 0) result = "-" + result;

				Int32 radix = 0;
				if(d > 0) {
					result += ".";
					while(d > 0) {
						if(radix++ >= p) break;
						d = d * 10;
						Char c = (Int32)d + CC_NUMBER_0;
						d -= (Int32)d;
						result = result + c;
					}
				}
				return result;
			}
	};
}

#endif
