#include "ares.hpp"
#include <cstdlib>
#include <utility>
#include <ctime>
#include <cmath>

using namespace ares;
using namespace std;

constexpr int K = 8;

struct kmeans_map {

	collection<pair<double, double>> points;

	void setup(collection<pair<double, double>> &&points) {
		this->points = std::move(points);
	}

	void map(const pair<double, double> &point, collection2<int, pair<double, double>> &result) {
		int min_k = -1;
		double min_dist = 7;
		for (int k = 0; k < (int)points.size(); k++)
		{
			pair<double, double> &base = points[k];
			double dist = sqrt((base.first - point.first) * (base.first - point.first) +
					(base.second - point.second) * (base.second - point.second));
			if (dist < min_dist)
			{
				min_k = k;
				min_dist = dist;
			}
		}

		result.emplace_back(min_k, point);
	}
};

struct kmeans_reduce {
	pair<double, double> reduce(int key, const collection<pair<double, double>> &values) {
		double posx = 0;
		double posy = 0;
		for (const pair<double, double> &value : values) {
			posx += value.first;
			posy += value.second;
		}

		posx /= values.size();
		posy /= values.size();
		return make_pair(posx, posy);
	}
};

int main(int argc, char **argv) {
	initialize<kmeans_map, kmeans_reduce>();

	collection<pair<double, double>> input;

	printf("start\n");

	double a, b;
	FILE *file = fopen(argv[1], "r");
	while (fscanf(file, "%lf%lf", &a, &b) != -1) {
		input.emplace_back(a, b);
	}
	fclose(file);

	printf("read finish\n");

	collection<pair<double, double>> result;

	for (int k = 0; k < K; k++) {
		result.push_back(input[k]);
	}

	scatter_map_data(std::move(input));

	for (int k = 0; k < 5; k++) {
		set_map_side_data(result);
		result = run_without_scatter<kmeans_map, kmeans_reduce>();

		printf("iteration %d finish\n", k);
		sort(result.begin(), result.end());
		for (int s = 0; s < K; s++) {
			printf("%.3f %.3f\n", result[s].first, result[s].second);
		}
		printf("--------------\n");
	}
	return 0;
}

