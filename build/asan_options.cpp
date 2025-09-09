extern "C" const char* __asan_default_options()
{
  return "exitcode=0:abort_on_error=0:halt_on_error=0:disable_coredump=0:print_stats=0:detect_odr_violation=0:check_initialization_order=1:detect_stack_use_after_return=1:atexit=1:detect_leaks=1:log_path=/core/binary.asan.log";
}   
