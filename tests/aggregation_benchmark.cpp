#pragma once

#include <iostream>
#include "benchmark/benchmark.h"

#include "datatypes/mcs_datatype.h"

#include "joblist/tupleaggregatestep.h"

class AggregationBenchmarkFixture : public benchmark::Fixture
{
 public:
  void SetUp(benchmark::State& state)
  {
    // read table
    // create jobinfo
    auto jobInfo = new JobInfo();
  }

  void TearDown(benchmark::State& state)
  {
  }
};

BENCHMARK_DEFINE_F(AggregationBenchmarkFixture, BM_SingleColumnGroupBy)(benchmark::State& state)
{
  auto jobStep = new JobStep(jobInfo);
  auto aggregationStep = TupleAggregateStep::prepAggregate();
  for (auto _ : state)
  {
    // aggregationStep.run();
    // aggregationStep.join();
  }
}

BENCHMARK_REGISTER_F(AggregationBenchmarkFixture, BM_SingleColumnGroupBy);

BENCHMARK_MAIN();