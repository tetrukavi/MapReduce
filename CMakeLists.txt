#cmake_minimum_required(VERSION 3.15)
#project(OS_EX_3)
#
#set(CMAKE_CXX_STANDARD 11)
#
#add_executable(OS_EX_3 Barrier.cpp Barrier.h MapReduceFramework.h MapReduceClient.h MapReduceFramework.cpp test_avi_lirom.cpp)

cmake_minimum_required(VERSION 3.15)
project(OS_EX_3)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG ON) # not sure what is the difference between these two lines, but stackoverflow....

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 11)

add_executable(OS_EX_3 SampleClient.cpp MapReduceFramework.cpp Barrier.cpp)
target_link_libraries(OS_EX_3 PRIVATE Threads::Threads)