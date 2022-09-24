/*
    pdqsort.h - Pattern-defeating quicksort.
    Copyright (c) 2021 Orson Peters
    This software is provided 'as-is', without any express or implied warranty. In no event will the
    authors be held liable for any damages arising from the use of this software.
    Permission is granted to anyone to use this software for any purpose, including commercial
    applications, and to alter it and redistribute it freely, subject to the following restrictions:
    1. The origin of this software must not be misrepresented; you must not claim that you wrote the
       original software. If you use this software in a product, an acknowledgment in the product
       documentation would be appreciated but is not required.
    2. Altered source versions must be plainly marked as such, and must not be misrepresented as
       being the original software.
    3. This notice may not be removed or altered from any source distribution.
*/

#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <utility>
#include <iterator>

#if __cplusplus >= 201103L
#include <cstdint>
#include <type_traits>
#define PDQSORT_PREFER_MOVE(x) std::move(x)
#else
#define PDQSORT_PREFER_MOVE(x) (x)
#endif

namespace modified_pdqsort_detail
{
enum
{
  // Partitions below this size are sorted using insertion sort.
  insertion_sort_threshold = 24,

  // Partitions above this size use Tukey's ninther to select the pivot.
  ninther_threshold = 128,

  // When we detect an already sorted partition, attempt an insertion sort that allows this
  // amount of element moves before giving up.
  partial_insertion_sort_limit = 8,

  // Must be multiple of 8 due to loop unrolling, and < 256 to fit in unsigned char.
  block_size = 64,

  // Cacheline size, assumes power of two.
  cacheline_size = 64

};

#if __cplusplus >= 201103L
template <class T>
struct is_default_compare : std::false_type
{
};
template <class T>
struct is_default_compare<std::less<T>> : std::true_type
{
};
template <class T>
struct is_default_compare<std::greater<T>> : std::true_type
{
};
#endif

// Returns floor(log2(n)), assumes n > 0.
template <class T>
inline int log2(T n)
{
  int log = 0;
  while (n >>= 1)
    ++log;
  return log;
}

// Sorts [begin, end) using insertion sort with the given comparison function.
template <class Iter, class Compare>
inline void insertion_sort(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  if (begin == end)
    return;

  for (Iter cur = begin + 1; cur != end; ++cur)
  {
    Iter sift = cur;
    Iter sift_1 = cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
      } while (sift != begin && comp(tmp, *--sift_1));

      *sift = PDQSORT_PREFER_MOVE(tmp);
    }
  }
}

template <class Iter, class PermIter, class Compare>
inline void mod_insertion_sort(Iter begin, Iter end, PermIter perm_begin, PermIter perm_end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  using PermT = typename std::iterator_traits<PermIter>::value_type;

  if (begin == end)
    return;

  PermIter perm_cur = perm_begin + 1;
  for (Iter cur = begin + 1; cur != end; ++cur, ++perm_cur)
  {
    Iter sift = cur;
    Iter sift_1 = cur - 1;
    PermIter perm_sift = perm_cur;
    PermIter perm_sift_1 = perm_cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);
      PermT perm_tmp = PDQSORT_PREFER_MOVE(*perm_sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
        *perm_sift-- = PDQSORT_PREFER_MOVE(*perm_sift_1);

      } while (sift != begin && comp(tmp, *--sift_1) && --perm_sift_1 != perm_end + 10);
      // WIP I presume --perm_sift_1 != sift_1 is always true

      *sift = PDQSORT_PREFER_MOVE(tmp);
      *perm_sift = PDQSORT_PREFER_MOVE(perm_tmp);
    }
  }
}

// Sorts [begin, end) using insertion sort with the given comparison function. Assumes
// *(begin - 1) is an element smaller than or equal to any element in [begin, end).
template <class Iter, class Compare>
inline void unguarded_insertion_sort(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  if (begin == end)
    return;

  for (Iter cur = begin + 1; cur != end; ++cur)
  {
    Iter sift = cur;
    Iter sift_1 = cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
      } while (comp(tmp, *--sift_1));

      *sift = PDQSORT_PREFER_MOVE(tmp);
    }
  }
}

// Sorts [begin, end) using insertion sort with the given comparison function. Assumes
// *(begin - 1) is an element smaller than or equal to any element in [begin, end).
template <class Iter, class PermIter, class Compare>
inline void mod_unguarded_insertion_sort(Iter begin, Iter end, PermIter perm_begin, PermIter perm_end,
                                         Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  using PermT = typename std::iterator_traits<PermIter>::value_type;

  if (begin == end)
    return;

  PermIter perm_cur = perm_begin + 1;
  for (Iter cur = begin + 1; cur != end; ++cur, ++perm_cur)
  {
    assert(perm_cur != perm_end);
    Iter sift = cur;
    Iter sift_1 = cur - 1;
    PermIter perm_sift = perm_cur;
    PermIter perm_sift_1 = perm_cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);
      PermT perm_tmp = PDQSORT_PREFER_MOVE(*perm_sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
        *perm_sift-- = PDQSORT_PREFER_MOVE(*perm_sift_1);
      } while (comp(tmp, *--sift_1) && --perm_sift_1 != perm_end + 10);

      *sift = PDQSORT_PREFER_MOVE(tmp);
      *perm_sift = PDQSORT_PREFER_MOVE(perm_tmp);
    }
  }
}

// Attempts to use insertion sort on [begin, end). Will return false if more than
// partial_insertion_sort_limit elements were moved, and abort sorting. Otherwise it will
// successfully sort and return true.
template <class Iter, class Compare>
inline bool partial_insertion_sort(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  if (begin == end)
    return true;

  std::size_t limit = 0;
  for (Iter cur = begin + 1; cur != end; ++cur)
  {
    Iter sift = cur;
    Iter sift_1 = cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
      } while (sift != begin && comp(tmp, *--sift_1));

      *sift = PDQSORT_PREFER_MOVE(tmp);
      limit += cur - sift;
    }

    if (limit > partial_insertion_sort_limit)
      return false;
  }

  return true;
}

template <class Iter, class PermIter, class Compare>
inline bool mod_partial_insertion_sort(Iter begin, Iter end, PermIter perm_begin, PermIter perm_end,
                                       Compare comp)
{
  using T = typename std::iterator_traits<Iter>::value_type;
  using PermT = typename std::iterator_traits<PermIter>::value_type;

  if (begin == end)
    return true;

  std::size_t limit = 0;
  PermIter perm_cur = perm_begin + 1;
  // WIP permutation and key array sizes must be identical
  for (Iter cur = begin + 1; cur != end; ++cur, ++perm_cur)
  {
    assert(perm_cur != perm_end);
    Iter sift = cur;
    Iter sift_1 = cur - 1;
    PermIter perm_sift = perm_cur;
    PermIter perm_sift_1 = perm_cur - 1;

    // Compare first so we can avoid 2 moves for an element already positioned correctly.
    if (comp(*sift, *sift_1))
    {
      T tmp = PDQSORT_PREFER_MOVE(*sift);
      PermT perm_tmp = PDQSORT_PREFER_MOVE(*perm_sift);

      do
      {
        *sift-- = PDQSORT_PREFER_MOVE(*sift_1);
        *perm_sift-- = PDQSORT_PREFER_MOVE(*perm_sift_1);
      } while (sift != begin && comp(tmp, *--sift_1) && --perm_sift_1 != perm_end + 10);
      // WIP I presume --perm_sift_1 != sift_1 is always true

      *sift = PDQSORT_PREFER_MOVE(tmp);
      *perm_sift = PDQSORT_PREFER_MOVE(perm_tmp);
      limit += cur - sift;
    }

    if (limit > partial_insertion_sort_limit)
      return false;
  }

  return true;
}

template <class Iter, class Compare>
inline void sort2(Iter a, Iter b, Compare comp)
{
  if (comp(*b, *a))
    std::iter_swap(a, b);
}

template <class Iter, class PermIter, class Compare>
inline void mod_sort2(Iter a, Iter b, PermIter pA, PermIter pB, Compare comp)
{
  if (comp(*b, *a))
  {
    std::iter_swap(a, b);
    std::iter_swap(pA, pB);
  }
}

// Sorts the elements *a, *b and *c using comparison function comp.
template <class Iter, class Compare>
inline void sort3(Iter a, Iter b, Iter c, Compare comp)
{
  sort2(a, b, comp);
  sort2(b, c, comp);
  sort2(a, b, comp);
}

template <class Iter, class PermIter, class Compare>
inline void mod_sort3(Iter a, Iter b, Iter c, PermIter pA, PermIter pB, PermIter pC, Compare comp)
{
  mod_sort2(a, b, pA, pB, comp);
  mod_sort2(b, c, pB, pC, comp);
  mod_sort2(a, b, pA, pB, comp);
}

template <class T>
inline T* align_cacheline(T* p)
{
#if defined(UINTPTR_MAX) && __cplusplus >= 201103L
  std::uintptr_t ip = reinterpret_cast<std::uintptr_t>(p);
#else
  std::size_t ip = reinterpret_cast<std::size_t>(p);
#endif
  ip = (ip + cacheline_size - 1) & -cacheline_size;
  return reinterpret_cast<T*>(ip);
}

template <class Iter>
inline void swap_offsets(Iter first, Iter last, unsigned char* offsets_l, unsigned char* offsets_r,
                         size_t num, bool use_swaps)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  if (use_swaps)
  {
    // This case is needed for the descending distribution, where we need
    // to have proper swapping for pdqsort to remain O(n).
    for (size_t i = 0; i < num; ++i)
    {
      std::iter_swap(first + offsets_l[i], last - offsets_r[i]);
    }
  }
  else if (num > 0)
  {
    Iter l = first + offsets_l[0];
    Iter r = last - offsets_r[0];
    T tmp(PDQSORT_PREFER_MOVE(*l));
    *l = PDQSORT_PREFER_MOVE(*r);
    for (size_t i = 1; i < num; ++i)
    {
      l = first + offsets_l[i];
      *r = PDQSORT_PREFER_MOVE(*l);
      r = last - offsets_r[i];
      *l = PDQSORT_PREFER_MOVE(*r);
    }
    *r = PDQSORT_PREFER_MOVE(tmp);
  }
}

// Partitions [begin, end) around pivot *begin using comparison function comp. Elements equal
// to the pivot are put in the right-hand partition. Returns the position of the pivot after
// partitioning and whether the passed sequence already was correctly partitioned. Assumes the
// pivot is a median of at least 3 elements and that [begin, end) is at least
// insertion_sort_threshold long. Uses branchless partitioning.
template <class Iter, class Compare>
inline std::pair<Iter, bool> partition_right_branchless(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;

  // Move pivot into local for speed.
  T pivot(PDQSORT_PREFER_MOVE(*begin));
  Iter first = begin;
  Iter last = end;

  // Find the first element greater than or equal than the pivot (the median of 3 guarantees
  // this exists).
  while (comp(*++first, pivot))
    ;

  // Find the first element strictly smaller than the pivot. We have to guard this search if
  // there was no element before *first.
  if (first - 1 == begin)
    while (first < last && !comp(*--last, pivot))
      ;
  else
    while (!comp(*--last, pivot))
      ;

  // If the first pair of elements that should be swapped to partition are the same element,
  // the passed in sequence already was correctly partitioned.
  bool already_partitioned = first >= last;
  if (!already_partitioned)
  {
    std::iter_swap(first, last);
    ++first;

    // The following branchless partitioning is derived from "BlockQuicksort: How Branch
    // Mispredictions don’t affect Quicksort" by Stefan Edelkamp and Armin Weiss, but
    // heavily micro-optimized.
    unsigned char offsets_l_storage[block_size + cacheline_size];
    unsigned char offsets_r_storage[block_size + cacheline_size];
    unsigned char* offsets_l = align_cacheline(offsets_l_storage);
    unsigned char* offsets_r = align_cacheline(offsets_r_storage);

    Iter offsets_l_base = first;
    Iter offsets_r_base = last;
    size_t num_l, num_r, start_l, start_r;
    num_l = num_r = start_l = start_r = 0;

    while (first < last)
    {
      // Fill up offset blocks with elements that are on the wrong side.
      // First we determine how much elements are considered for each offset block.
      size_t num_unknown = last - first;
      size_t left_split = num_l == 0 ? (num_r == 0 ? num_unknown / 2 : num_unknown) : 0;
      size_t right_split = num_r == 0 ? (num_unknown - left_split) : 0;

      // Fill the offset blocks.
      if (left_split >= block_size)
      {
        for (size_t i = 0; i < block_size;)
        {
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
        }
      }
      else
      {
        for (size_t i = 0; i < left_split;)
        {
          offsets_l[num_l] = i++;
          num_l += !comp(*first, pivot);
          ++first;
        }
      }

      if (right_split >= block_size)
      {
        for (size_t i = 0; i < block_size;)
        {
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
        }
      }
      else
      {
        for (size_t i = 0; i < right_split;)
        {
          offsets_r[num_r] = ++i;
          num_r += comp(*--last, pivot);
        }
      }

      // Swap elements and update block sizes and first/last boundaries.
      size_t num = std::min(num_l, num_r);
      swap_offsets(offsets_l_base, offsets_r_base, offsets_l + start_l, offsets_r + start_r, num,
                   num_l == num_r);
      num_l -= num;
      num_r -= num;
      start_l += num;
      start_r += num;

      if (num_l == 0)
      {
        start_l = 0;
        offsets_l_base = first;
      }

      if (num_r == 0)
      {
        start_r = 0;
        offsets_r_base = last;
      }
    }

    // We have now fully identified [first, last)'s proper position. Swap the last elements.
    if (num_l)
    {
      offsets_l += start_l;
      while (num_l--)
        std::iter_swap(offsets_l_base + offsets_l[num_l], --last);
      first = last;
    }
    if (num_r)
    {
      offsets_r += start_r;
      while (num_r--)
        std::iter_swap(offsets_r_base - offsets_r[num_r], first), ++first;
      last = first;
    }
  }

  // Put the pivot in the right place.
  Iter pivot_pos = first - 1;
  *begin = PDQSORT_PREFER_MOVE(*pivot_pos);
  *pivot_pos = PDQSORT_PREFER_MOVE(pivot);

  return std::make_pair(pivot_pos, already_partitioned);
}

// Partitions [begin, end) around pivot *begin using comparison function comp. Elements equal
// to the pivot are put in the right-hand partition. Returns the position of the pivot after
// partitioning and whether the passed sequence already was correctly partitioned. Assumes the
// pivot is a median of at least 3 elements and that [begin, end) is at least
// insertion_sort_threshold long.
template <class Iter, class Compare>
inline std::pair<Iter, bool> partition_right(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;

  // Move pivot into local for speed.
  T pivot(PDQSORT_PREFER_MOVE(*begin));

  Iter first = begin;
  Iter last = end;

  // Find the first element greater than or equal than the pivot (the median of 3 guarantees
  // this exists).
  while (comp(*++first, pivot))
    ;

  // Find the first element strictly smaller than the pivot. We have to guard this search if
  // there was no element before *first.
  if (first - 1 == begin)
    while (first < last && !comp(*--last, pivot))
      ;
  else
    while (!comp(*--last, pivot))
      ;

  // If the first pair of elements that should be swapped to partition are the same element,
  // the passed in sequence already was correctly partitioned.
  bool already_partitioned = first >= last;

  // Keep swapping pairs of elements that are on the wrong side of the pivot. Previously
  // swapped pairs guard the searches, which is why the first iteration is special-cased
  // above.
  while (first < last)
  {
    std::iter_swap(first, last);
    while (comp(*++first, pivot))
      ;
    while (!comp(*--last, pivot))
      ;
  }

  // Put the pivot in the right place.
  Iter pivot_pos = first - 1;
  *begin = PDQSORT_PREFER_MOVE(*pivot_pos);
  *pivot_pos = PDQSORT_PREFER_MOVE(pivot);

  return std::make_pair(pivot_pos, already_partitioned);
}

// insertion_sort_threshold long.
template <class Iter, class PermIter, class Compare>
inline std::tuple<Iter, PermIter, bool> mod_partition_right(Iter begin, Iter end, PermIter perm_begin,
                                                            PermIter perm_end, Compare comp)
{
  using T = typename std::iterator_traits<Iter>::value_type;
  using PermT = typename std::iterator_traits<PermIter>::value_type;

  // Move pivot into local for speed.
  T pivot(PDQSORT_PREFER_MOVE(*begin));
  PermT perm_pivot(PDQSORT_PREFER_MOVE(*perm_begin));

  Iter first = begin;
  Iter last = end;

  // Find the first element greater than or equal than the pivot (the median of 3 guarantees
  // this exists).
  while (comp(*++first, pivot))
    ;

  // Find the first element strictly smaller than the pivot. We have to guard this search if
  // there was no element before *first.
  if (first - 1 == begin)
    while (first < last && !comp(*--last, pivot))
      ;
  else
    while (!comp(*--last, pivot))
      ;

  // If the first pair of elements that should be swapped to partition are the same element,
  // the passed in sequence already was correctly partitioned.
  bool already_partitioned = first >= last;

  // Keep swapping pairs of elements that are on the wrong side of the pivot. Previously
  // swapped pairs guard the searches, which is why the first iteration is special-cased
  // above.
  while (first < last)
  {
    auto perm_first_dist = std::distance(begin, first);
    auto perm_last_dist = std::distance(begin, last);
    PermIter perm_first = perm_begin + perm_first_dist;
    PermIter perm_last = perm_begin + perm_last_dist;
    std::iter_swap(first, last);
    std::iter_swap(perm_first, perm_last);
    while (comp(*++first, pivot))
      ;
    while (!comp(*--last, pivot))
      ;
  }

  // Put the pivot in the right place.
  Iter pivot_pos = first - 1;
  auto dist = std::distance(begin, pivot_pos);
  PermIter perm_pivot_pos = perm_begin + dist;
  *begin = PDQSORT_PREFER_MOVE(*pivot_pos);
  *perm_begin = PDQSORT_PREFER_MOVE(*perm_pivot_pos);
  *pivot_pos = PDQSORT_PREFER_MOVE(pivot);
  *perm_pivot_pos = PDQSORT_PREFER_MOVE(perm_pivot);

  return {pivot_pos, perm_pivot_pos, already_partitioned};
}

// Similar function to the one above, except elements equal to the pivot are put to the left of
// the pivot and it doesn't check or return if the passed sequence already was partitioned.
// Since this is rarely used (the many equal case), and in that case pdqsort already has O(n)
// performance, no block quicksort is applied here for simplicity.
template <class Iter, class Compare>
inline Iter partition_left(Iter begin, Iter end, Compare comp)
{
  typedef typename std::iterator_traits<Iter>::value_type T;

  T pivot(PDQSORT_PREFER_MOVE(*begin));
  Iter first = begin;
  Iter last = end;

  while (comp(pivot, *--last))
    ;

  if (last + 1 == end)
    while (first < last && !comp(pivot, *++first))
      ;
  else
    while (!comp(pivot, *++first))
      ;

  while (first < last)
  {
    std::iter_swap(first, last);
    while (comp(pivot, *--last))
      ;
    while (!comp(pivot, *++first))
      ;
  }

  Iter pivot_pos = last;
  *begin = PDQSORT_PREFER_MOVE(*pivot_pos);
  *pivot_pos = PDQSORT_PREFER_MOVE(pivot);

  return pivot_pos;
}

template <class Iter, class PermIter, class Compare>
inline std::pair<Iter, PermIter> mod_partition_left(Iter begin, Iter end, PermIter perm_begin,
                                                    PermIter perm_end, Compare comp)
{
  using T = typename std::iterator_traits<Iter>::value_type;
  using PermT = typename std::iterator_traits<PermIter>::value_type;

  T pivot(PDQSORT_PREFER_MOVE(*begin));
  PermT perm_pivot(PDQSORT_PREFER_MOVE(*begin));
  Iter first = begin;
  Iter last = end;

  while (comp(pivot, *--last))
    ;

  if (last + 1 == end)
    while (first < last && !comp(pivot, *++first))
      ;
  else
    while (!comp(pivot, *++first))
      ;

  while (first < last)
  {
    auto perm_first_dist = std::distance(begin, first);
    auto perm_last_dist = std::distance(begin, last);
    PermIter perm_first = perm_begin + perm_first_dist;
    PermIter perm_last = perm_begin + perm_last_dist;
    std::iter_swap(first, last);
    std::iter_swap(perm_first, perm_last);

    while (comp(pivot, *--last))
      ;
    while (!comp(pivot, *++first))
      ;
  }

  Iter pivot_pos = last;
  auto dist = std::distance(begin, pivot_pos);
  PermIter perm_pivot_pos = perm_begin + dist;
  *begin = PDQSORT_PREFER_MOVE(*pivot_pos);
  *perm_begin = PDQSORT_PREFER_MOVE(*perm_pivot_pos);
  *pivot_pos = PDQSORT_PREFER_MOVE(pivot);
  *perm_pivot_pos = PDQSORT_PREFER_MOVE(perm_pivot);

  return {pivot_pos, perm_pivot_pos};
}

template <class Iter, class Compare, bool Branchless>
inline void pdqsort_loop(Iter begin, Iter end, Compare comp, int bad_allowed, bool leftmost = true)
{
  typedef typename std::iterator_traits<Iter>::difference_type diff_t;

  // Use a while loop for tail recursion elimination.
  while (true)
  {
    diff_t size = end - begin;

    // Insertion sort is faster for small arrays.
    if (size < insertion_sort_threshold)
    {
      if (leftmost)
        insertion_sort(begin, end, comp);
      else
        unguarded_insertion_sort(begin, end, comp);
      return;
    }

    // Choose pivot as median of 3 or pseudomedian of 9.
    diff_t s2 = size / 2;
    if (size > ninther_threshold)
    {
      sort3(begin, begin + s2, end - 1, comp);
      sort3(begin + 1, begin + (s2 - 1), end - 2, comp);
      sort3(begin + 2, begin + (s2 + 1), end - 3, comp);
      sort3(begin + (s2 - 1), begin + s2, begin + (s2 + 1), comp);
      std::iter_swap(begin, begin + s2);
    }
    else
      sort3(begin + s2, begin, end - 1, comp);

    // If *(begin - 1) is the end of the right partition of a previous partition operation
    // there is no element in [begin, end) that is smaller than *(begin - 1). Then if our
    // pivot compares equal to *(begin - 1) we change strategy, putting equal elements in
    // the left partition, greater elements in the right partition. We do not have to
    // recurse on the left partition, since it's sorted (all equal).
    if (!leftmost && !comp(*(begin - 1), *begin))
    {
      begin = partition_left(begin, end, comp) + 1;
      continue;
    }

    // Partition and get results.
    std::pair<Iter, bool> part_result =
        Branchless ? partition_right_branchless(begin, end, comp) : partition_right(begin, end, comp);

    Iter pivot_pos = part_result.first;
    bool already_partitioned = part_result.second;

    // Check for a highly unbalanced partition.
    diff_t l_size = pivot_pos - begin;
    diff_t r_size = end - (pivot_pos + 1);
    bool highly_unbalanced = l_size < size / 8 || r_size < size / 8;

    // If we got a highly unbalanced partition we shuffle elements to break many patterns.
    if (highly_unbalanced)
    {
      // If we had too many bad partitions, switch to heapsort to guarantee O(n log n).
      if (--bad_allowed == 0)
      {
        std::make_heap(begin, end, comp);
        std::sort_heap(begin, end, comp);
        return;
      }

      if (l_size >= insertion_sort_threshold)
      {
        std::iter_swap(begin, begin + l_size / 4);
        std::iter_swap(pivot_pos - 1, pivot_pos - l_size / 4);

        if (l_size > ninther_threshold)
        {
          std::iter_swap(begin + 1, begin + (l_size / 4 + 1));
          std::iter_swap(begin + 2, begin + (l_size / 4 + 2));
          std::iter_swap(pivot_pos - 2, pivot_pos - (l_size / 4 + 1));
          std::iter_swap(pivot_pos - 3, pivot_pos - (l_size / 4 + 2));
        }
      }

      if (r_size >= insertion_sort_threshold)
      {
        std::iter_swap(pivot_pos + 1, pivot_pos + (1 + r_size / 4));
        std::iter_swap(end - 1, end - r_size / 4);

        if (r_size > ninther_threshold)
        {
          std::iter_swap(pivot_pos + 2, pivot_pos + (2 + r_size / 4));
          std::iter_swap(pivot_pos + 3, pivot_pos + (3 + r_size / 4));
          std::iter_swap(end - 2, end - (1 + r_size / 4));
          std::iter_swap(end - 3, end - (2 + r_size / 4));
        }
      }
    }
    else
    {
      // If we were decently balanced and we tried to sort an already partitioned
      // sequence try to use insertion sort.
      if (already_partitioned && partial_insertion_sort(begin, pivot_pos, comp) &&
          partial_insertion_sort(pivot_pos + 1, end, comp))
        return;
    }

    // Sort the left partition first using recursion and do tail recursion elimination for
    // the right-hand partition.
    pdqsort_loop<Iter, Compare, Branchless>(begin, pivot_pos, comp, bad_allowed, leftmost);
    begin = pivot_pos + 1;
    leftmost = false;
  }
}

template <class Iter, class PermIter, class Compare, bool Branchless>
inline void mod_pdqsort_loop(Iter begin, Iter end, PermIter perm_begin, PermIter perm_end, Compare comp,
                             int bad_allowed, bool leftmost = true)
{
  typedef typename std::iterator_traits<Iter>::difference_type diff_t;

  // Use a while loop for tail recursion elimination.
  while (true)
  {
    diff_t size = end - begin;

    // Insertion sort is faster for small arrays.
    if (size < insertion_sort_threshold)
    {
      if (leftmost)
        mod_insertion_sort(begin, end, perm_begin, perm_end, comp);
      else
        mod_unguarded_insertion_sort(begin, end, perm_begin, perm_end, comp);
      return;
    }

    // Choose pivot as median of 3 or pseudomedian of 9.
    diff_t s2 = size / 2;
    if (size > ninther_threshold)
    {
      mod_sort3(begin, begin + s2, end - 1, perm_begin, perm_begin + s2, perm_end - 1, comp);
      mod_sort3(begin + 1, begin + (s2 - 1), end - 2, perm_begin + 1, perm_begin + (s2 - 1), perm_end - 2,
                comp);
      mod_sort3(begin + 2, begin + (s2 + 1), end - 3, perm_begin + 2, perm_begin + (s2 + 1), perm_end - 3,
                comp);
      mod_sort3(begin + (s2 - 1), begin + s2, begin + (s2 + 1), perm_begin + (s2 - 1), perm_begin + s2,
                perm_begin + (s2 + 1), comp);
      std::iter_swap(begin, begin + s2);
      std::iter_swap(perm_begin, perm_begin + s2);
    }
    else
      mod_sort3(begin + s2, begin, end - 1, perm_begin + s2, perm_begin, perm_end - 1, comp);

    // If *(begin - 1) is the end of the right partition of a previous partition operation
    // there is no element in [begin, end) that is smaller than *(begin - 1). Then if our
    // pivot compares equal to *(begin - 1) we change strategy, putting equal elements in
    // the left partition, greater elements in the right partition. We do not have to
    // recurse on the left partition, since it's sorted (all equal).
    if (!leftmost && !comp(*(begin - 1), *begin))
    {
      auto [begin_, perm_begin_] = mod_partition_left(begin, end, perm_begin, perm_end, comp);
      begin = PDQSORT_PREFER_MOVE(++begin_);
      perm_begin = PDQSORT_PREFER_MOVE(++perm_begin);
      continue;
    }

    // Partition and get results.
    // std::pair<Iter, bool> part_result =
    //     Branchless ? partition_right_branchless(begin, end, comp) : partition_right(begin, end, comp);
    auto [pivot_pos_, perm_pivot_pos_, already_partitioned] =
        mod_partition_right(begin, end, perm_begin, perm_end, comp);

    Iter pivot_pos = PDQSORT_PREFER_MOVE(pivot_pos_);
    PermIter perm_pivot_pos = PDQSORT_PREFER_MOVE(perm_pivot_pos_);

    // Check for a highly unbalanced partition.
    diff_t l_size = pivot_pos - begin;
    diff_t r_size = end - (pivot_pos + 1);
    bool highly_unbalanced = l_size < size / 8 || r_size < size / 8;

    // If we got a highly unbalanced partition we shuffle elements to break many patterns.
    if (highly_unbalanced)
    {
      // If we had too many bad partitions, switch to heapsort to guarantee O(n log n).
      // WIP
      // if (--bad_allowed == 0)
      // {
      //   std::make_heap(begin, end, comp);
      //   std::sort_heap(begin, end, comp);
      //   return;
      // }

      if (l_size >= insertion_sort_threshold)
      {
        std::iter_swap(begin, begin + l_size / 4);
        std::iter_swap(perm_begin, perm_begin + l_size / 4);

        std::iter_swap(pivot_pos - 1, pivot_pos - l_size / 4);
        std::iter_swap(perm_pivot_pos - 1, perm_pivot_pos - l_size / 4);

        if (l_size > ninther_threshold)
        {
          std::iter_swap(begin + 1, begin + (l_size / 4 + 1));
          std::iter_swap(perm_begin + 1, perm_begin + (l_size / 4 + 1));

          std::iter_swap(begin + 2, begin + (l_size / 4 + 2));
          std::iter_swap(perm_begin + 2, perm_begin + (l_size / 4 + 2));

          std::iter_swap(pivot_pos - 2, pivot_pos - (l_size / 4 + 1));
          std::iter_swap(perm_pivot_pos - 2, perm_pivot_pos - (l_size / 4 + 1));

          std::iter_swap(pivot_pos - 3, pivot_pos - (l_size / 4 + 2));
          std::iter_swap(perm_pivot_pos - 3, perm_pivot_pos - (l_size / 4 + 2));
        }
      }

      if (r_size >= insertion_sort_threshold)
      {
        std::iter_swap(pivot_pos + 1, pivot_pos + (1 + r_size / 4));
        std::iter_swap(perm_pivot_pos + 1, perm_pivot_pos + (1 + r_size / 4));

        std::iter_swap(end - 1, end - r_size / 4);
        std::iter_swap(perm_end - 1, perm_end - r_size / 4);

        if (r_size > ninther_threshold)
        {
          std::iter_swap(pivot_pos + 2, pivot_pos + (2 + r_size / 4));
          std::iter_swap(perm_pivot_pos + 2, perm_pivot_pos + (2 + r_size / 4));

          std::iter_swap(pivot_pos + 3, pivot_pos + (3 + r_size / 4));
          std::iter_swap(perm_pivot_pos + 3, perm_pivot_pos + (3 + r_size / 4));

          std::iter_swap(end - 2, end - (1 + r_size / 4));
          std::iter_swap(perm_end - 2, perm_end - (1 + r_size / 4));

          std::iter_swap(end - 3, end - (2 + r_size / 4));
          std::iter_swap(perm_end - 3, perm_end - (2 + r_size / 4));
        }
      }
    }
    else
    {
      // If we were decently balanced and we tried to sort an already partitioned
      // sequence try to use insertion sort.
      if (already_partitioned &&
          mod_partial_insertion_sort(begin, pivot_pos, perm_begin, perm_pivot_pos, comp) &&
          mod_partial_insertion_sort(pivot_pos + 1, end, perm_pivot_pos + 1, perm_end, comp))
        return;
    }

    // Sort the left partition first using recursion and do tail recursion elimination for
    // the right-hand partition.
    mod_pdqsort_loop<Iter, PermIter, Compare, Branchless>(begin, pivot_pos, perm_begin, perm_end, comp,
                                                          bad_allowed, leftmost);
    begin = pivot_pos + 1;
    perm_begin = perm_pivot_pos + 1;
    leftmost = false;
  }
}

}  // namespace modified_pdqsort_detail

namespace sorting
{
template <class Iter, class Compare>
inline void pdqsort(Iter begin, Iter end, Compare comp)
{
  if (begin == end)
    return;

#if __cplusplus >= 201103L
  modified_pdqsort_detail::pdqsort_loop<
      Iter, Compare,
      modified_pdqsort_detail::is_default_compare<typename std::decay<Compare>::type>::value &&
          std::is_arithmetic<typename std::iterator_traits<Iter>::value_type>::value>(
      begin, end, comp, modified_pdqsort_detail::log2(end - begin));
#else
  modified_pdqsort_detail::pdqsort_loop<Iter, Compare, false>(begin, end, comp,
                                                              modified_pdqsort_detail::log2(end - begin));
#endif
}

template <class Iter, class PermIter, class Compare>
inline void mod_pdqsort(Iter begin, Iter end, PermIter perm_begin, PermIter perm_end, Compare comp)
{
  if (begin == end)
    return;

  modified_pdqsort_detail::mod_pdqsort_loop<
      Iter, PermIter, Compare,
      modified_pdqsort_detail::is_default_compare<typename std::decay<Compare>::type>::value &&
          std::is_arithmetic<typename std::iterator_traits<Iter>::value_type>::value>(
      begin, end, perm_begin, perm_end, comp, modified_pdqsort_detail::log2(end - begin));
}

template <class Iter>
inline void pdqsort(Iter begin, Iter end)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  pdqsort(begin, end, std::less<T>());
}

template <class Iter, class Compare>
inline void pdqsort_branchless(Iter begin, Iter end, Compare comp)
{
  if (begin == end)
    return;
  modified_pdqsort_detail::pdqsort_loop<Iter, Compare, true>(begin, end, comp,
                                                             modified_pdqsort_detail::log2(end - begin));
}

template <class Iter>
inline void pdqsort_branchless(Iter begin, Iter end)
{
  typedef typename std::iterator_traits<Iter>::value_type T;
  pdqsort_branchless(begin, end, std::less<T>());
}
}  // end of namespace sorting

#undef PDQSORT_PREFER_MOVE
