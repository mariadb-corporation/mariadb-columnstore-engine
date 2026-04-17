set(rbo_SRCS
    rulebased_optimizer.cpp
    common_leaf_conjunctions.cpp
    check_filters_limit.cpp
    rbo_apply_parallel_ces.cpp
    rbo_apply_rewrite_distinct.cpp
    rbo_common_leaf_conjunctions_to_top.cpp
    rbo_predicate_pushdown.cpp
    rbo_decorrelate_outer_join_sub.cpp
    rbo_groupby_wrap_columns.cpp
    lib/parse_tree_ops.cpp)

columnstore_library(rbo ${rbo_SRCS})

target_include_directories(rbo PUBLIC ${ENGINE_COMMON_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR})
columnstore_link(rbo PUBLIC execplan)
