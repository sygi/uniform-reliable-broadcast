all: da_proc

da_proc: main.cpp
	g++ -O2 -Wall -o da_proc main.cpp -std=c++11 -pthread

clean:
	rm da_proc
