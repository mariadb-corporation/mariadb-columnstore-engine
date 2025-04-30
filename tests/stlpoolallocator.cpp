#include <gtest/gtest.h>
#include <vector>
#include <thread>
#include <memory>
#include "stlpoolallocator.h"
#include "resourcemanager.h"

using namespace utils;

static const constexpr int64_t MemoryAllowance = 1 * 1024 * 1024;


/**
 * Test fixture for STLPoolAllocator tests
 */
class STLPoolAllocatorTest : public ::testing::Test
{
protected:
    using TestType = int;  // Type we'll use for testing
    using Allocator = STLPoolAllocator<TestType>;
};

/**
 * Test default constructor and basic properties
 */
TEST_F(STLPoolAllocatorTest, DefaultConstructor)
{
    Allocator alloc;
    EXPECT_EQ(alloc.getMemUsage(), 0ULL);
    EXPECT_EQ(alloc.max_size(), std::numeric_limits<uint64_t>::max() / sizeof(TestType));
}

/**
 * Test basic allocation and deallocation
 */
TEST_F(STLPoolAllocatorTest, BasicAllocation)
{
    Allocator alloc;
    const size_t count = 10;
    uint64_t initialUsage = alloc.getMemUsage();
    
    // Allocate memory for 10 integers
    TestType* ptr = alloc.allocate(count);
    ASSERT_NE(ptr, nullptr);
    EXPECT_GT(alloc.getMemUsage(), initialUsage);
    
    // Construct objects
    for (size_t i = 0; i < count; ++i)
    {
        alloc.construct(ptr + i, static_cast<TestType>(i));
    }
    
    // Verify constructed values
    for (size_t i = 0; i < count; ++i)
    {
        EXPECT_EQ(ptr[i], static_cast<TestType>(i));
    }
    
    // Destroy objects
    for (size_t i = 0; i < count; ++i)
    {
        alloc.destroy(ptr + i);
    }
    
    // Deallocate memory
    alloc.deallocate(ptr, count);
}

/**
 * Test copy constructor and assignment operator
 */
TEST_F(STLPoolAllocatorTest, CopyAndAssignment)
{
    Allocator alloc1;
    TestType* ptr = alloc1.allocate(1);
    alloc1.construct(ptr, 42);
    
    // Test copy constructor
    Allocator alloc2(alloc1);
    EXPECT_EQ(alloc1.getMemUsage(), alloc2.getMemUsage());
    
    // Test assignment operator
    Allocator alloc3;
    alloc3 = alloc1;
    EXPECT_EQ(alloc1.getMemUsage(), alloc3.getMemUsage());
    
    alloc1.destroy(ptr);
    alloc1.deallocate(ptr, 1);
}

/**
 * Test STL container integration (vector)
 */
TEST_F(STLPoolAllocatorTest, VectorIntegration)
{
    std::vector<TestType, Allocator> vec;
    const size_t testSize = 1000;
    
    // Reserve to avoid reallocation
    vec.reserve(testSize);
    
    // Add elements
    for (size_t i = 0; i < testSize; ++i)
    {
        vec.push_back(static_cast<TestType>(i));
    }
    
    // Verify elements
    for (size_t i = 0; i < testSize; ++i)
    {
        EXPECT_EQ(vec[i], static_cast<TestType>(i));
    }
    
    // Clear vector
    vec.clear();
    vec.shrink_to_fit();
}

/**
 * Test ResourceManager integration
 */
TEST_F(STLPoolAllocatorTest, ResourceManagerIntegration)
{
    using TestType = int8_t;
    using Allocator = STLPoolAllocator<TestType>;

    joblist::ResourceManager rm;
    // To set the memory allowance
    rm.setMemory(MemoryAllowance);
    Allocator alloc(&rm, 1024, 512);
    
    TestType* ptr = alloc.allocate(1);
    ASSERT_NE(ptr, nullptr);

    alloc.construct(ptr, 42);
    EXPECT_EQ(*ptr, 42);
    
    alloc.destroy(ptr);
    alloc.deallocate(ptr, 1);

    TestType* ptr2 = alloc.allocate(65537);
    ASSERT_NE(ptr2, nullptr);
    alloc.construct(ptr2, 42);
    EXPECT_EQ(*ptr2, 42);

    
    alloc.destroy(ptr2);
    alloc.deallocate(ptr2, 1);
}

/**
 * Test rebind capability (required by STL)
 */
TEST_F(STLPoolAllocatorTest, RebindTest)
{
    using DoubleAllocator = typename Allocator::template rebind<double>::other;
    DoubleAllocator doubleAlloc;
    
    // Allocate and construct a double
    double* ptr = doubleAlloc.allocate(1);
    ASSERT_NE(ptr, nullptr);
    
    doubleAlloc.construct(ptr, 3.14159);
    EXPECT_DOUBLE_EQ(*ptr, 3.14159);
    
    doubleAlloc.destroy(ptr);
    doubleAlloc.deallocate(ptr, 1);
}
