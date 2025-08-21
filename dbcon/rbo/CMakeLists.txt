set(rbo_SRCS rulebased_optimizer.cpp rbo_apply_parallel_ces.cpp rbo_predicate_pushdown.cpp)

columnstore_library(rbo ${rbo_SRCS})

target_include_directories(rbo PUBLIC ${ENGINE_COMMON_INCLUDES})
columnstore_link(rbo PUBLIC execplan)
