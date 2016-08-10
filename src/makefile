all: main

main: main.o fileformat.pb.o osmformat.pb.o
	g++ -o main main.o fileformat.pb.o osmformat.pb.o -lprotobuf -lz

main.o: main.cpp
	g++ -c -std=c++11 main.cpp

fileformat.pb.o: ../headers/fileformat.pb.cc
	g++ -c ../headers/fileformat.pb.cc

osmformat.pb.o: ../headers/osmformat.pb.cc
	g++ -c ../headers/osmformat.pb.cc

clean:
	rm *.o main