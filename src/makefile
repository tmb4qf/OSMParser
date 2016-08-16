all: main

main: main.o fileformat.pb.o osmformat.pb.o
	g++ --std=c++11 -o main main.o fileformat.pb.o osmformat.pb.o -lprotobuf -lz $$(pkg-config --cflags --libs libmongocxx)

main.o: main.cpp ../headers/osmParser.h
	g++ -c --std=c++11 main.cpp $$(pkg-config --cflags --libs libmongocxx)

fileformat.pb.o: ../headers/fileformat.pb.cc
	g++ -c ../headers/fileformat.pb.cc

osmformat.pb.o: ../headers/osmformat.pb.cc
	g++ -c ../headers/osmformat.pb.cc

clean:
	rm *.o main