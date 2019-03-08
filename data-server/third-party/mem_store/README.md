Skiplist
--------
A generic [Skiplist](https://en.wikipedia.org/wiki/Skip_list) container C implementation, lock-free for both multiple readers and writers. It can be used as a set or a map, containing any type of data.

It basically uses STL atomic variables with C++ compiler, but they can be switched to built-in GCC atomic operations when we compile it with pure C compiler.

This repository also includes STL-style lock-free `set` and `map` containers, based on Skiplist implementation. 


Author
------
Jung-Sang Ahn <jungsang.ahn@gmail.com>


Build
-----
```sh
$ make
```

How to use
----------

Copy [`skiplist.cc`](src/skiplist.cc) file and [`include`](include) files to your source repository.

Or, use library file (`libskiplist.so` or `libskiplist.a`).

* Pure C

[examples/pure_c_example.c](examples/pure_c_example.c)

* C++ (STL-style `set` and `map`)

[examples/cpp_set_example.cc](examples/cpp_set_example.cc)

[examples/cpp_map_example.cc](examples/cpp_map_example.cc)


Benchmark results
-----------------
* Skiplist vs. STL set + STL mutex
* Single writer and multiple readers
* Randomly insert and read 100K integers

![alt text](https://github.com/greensky00/skiplist/blob/master/docs/swmr_graph.png "Throughput")
