
#ifndef _ARES_HELPER_HPP_
#define _ARES_HELPER_HPP_

#include "types.hpp"

#include <algorithm>
#include <fstream>
#include <string>

namespace ares_impl {

	namespace helper {

		template <typename F>
		collection<std::string> select(const std::string &input, F &&f) {
			collection<std::string> result;
			auto b = input.begin(), e = input.begin();
			while ( b != input.end() ) {
				b = std::find_if(b, input.end(), f);
				e = std::find_if_not(b, input.end(), f);
				if ( b != e) {
					result.emplace_back(b, e);
				}
				b = e;
			}
			return std::move(result);
		}

		collection<std::string> split(const std::string &input) {
			return select(input, isalnum);
		}

		collection<std::string> readfile(const std::string &name) {
			collection<std::string> result;
			std::ifstream in(name);
			std::string line;
			while ( getline(in, line) ) {
				result.push_back(line);
			}
			return std::move(result);
		}
	}
}

#endif // _ARES_HELPER_HPP_
