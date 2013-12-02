#include "ares.hpp"
#include <utility>
#include <ctime>

using namespace ares;
using namespace std;

/* requirement of serializable:
 *   constructible by ares::byte_array &
 *   function void write_to(ares::byte_array &)
 *   move constructible
 *   move assignable
 *
 * or: trivial
 * or: std::string
 * or: std::pair<serializable, serializable>
 * or: ares::collection<serializable>
 *
 */

class nc_int {
public:
	int v;
	nc_int() = default;
	nc_int(int v): v(v) {}

	nc_int(const nc_int &) = delete;
	nc_int &operator=(const nc_int &) = delete;

	nc_int(nc_int &&) = default;
	nc_int &operator=(nc_int &&) = default;

	nc_int(byte_array &bs): v(bs.read<int>()) {}
	void write_to(byte_array &bs) const { bs.write(v); }

	operator int() const { return v; }
};

typedef nc_int int_t;

struct word_count {
	void map(const string &input, collection2<string, int_t> &result) {
		collection<string> parts = helper::split(input);
		for (const string &part : parts) {
				result.emplace_back(part, 1);
		}
	}

	pair<string, int_t> reduce(const string &key, const collection<int_t> &values) {
		int count = 0;
		for (const int_t &value: values) {
			count += value;
		}
		return make_pair(key, count);
	}

	pair<string, int_t> combine(const string &key, const collection<int_t> &values) {
		return reduce(key, values);
	}
};

int main(int argc, char **argv) {
	initialize<word_count>();
	collection<string> input = helper::readfile(argv[1]);
	if (argc == 2) {
		printf("without combine\n");
		collection2<string, int_t> result1 = run_job<word_count, word_count, void>(input);
	} else {
		collection2<string, int_t> result2 = run_job<word_count>(input);
	}
	return 0;
}

