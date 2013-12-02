
CXXFLAGS = -O3 -Wall -std=c++11 -I./framework
FRAMEWORK = $(shell find framework -type f)

MPICXX = mpicxx 

all: wordcount kmeans

wordcount: example/wordcount.cpp $(FRAMEWORK)
	$(MPICXX) $(CXXFLAGS) -o $@ $< 

kmeans: example/kmeans.cpp $(FRAMEWORK)
	$(MPICXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f wordcount kmeans

