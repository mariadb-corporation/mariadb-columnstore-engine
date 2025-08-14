set(rbo_SRCS ../mysql/rulebased_optimizer.cpp ../mysql/rbo_apply_parallel_ces.cpp ../mysql/rbo_predicate_pushdown.cpp)

columnstore_library(rbo ${rbo_SRCS})

target_include_directories(rbo PUBLIC ${ENGINE_COMMON_INCLUDES})
columnstore_link(rbo PUBLIC execplan)
