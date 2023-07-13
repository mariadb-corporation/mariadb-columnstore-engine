cmake_minimum_required(VERSION 3.12)
project(Tutorial)

set(CMAKE_VERBOSE_MAKEFILE ON)

# Fuzzer targets
ADD_EXECUTABLE(mytarget_fuzz fuzz_target.cpp)
TARGET_COMPILE_OPTIONS(mytarget_fuzz PRIVATE -fsanitize=fuzzer)
TARGET_LINK_LIBRARIES(mytarget_fuzz PRIVATE libFuzzer.a)
