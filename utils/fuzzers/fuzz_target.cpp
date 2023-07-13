// test_fuzzer.cc
#include <stdint.h>
#include <stddef.h>
#include <iostream>

// Include the header file for the library
// extern "C" void hello();

// Function to be fuzzed
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
  if (size > 0 && data[0] == 'H')
    if (size > 1 && data[1] == 'I')
      if (size > 2 && data[2] == '!')
      {
        std::cout << "Fuzzing test succeeded!" << std::endl;
        hello(); // Call the function from the shared library
        __builtin_trap();
      }
  return 0;
}
