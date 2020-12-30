/*
   Copyright (C) 2020 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. 
*/ 
#ifndef MCS_TSFLOAT128_H_INCLUDED
#define MCS_TSFLOAT128_H_INCLUDED

#include <cfloat>
#include <cstdint>
#include <cstring>


namespace datatypes
{

/* Main union type we use to manipulate the floating-point type.  */
typedef union
{
  __float128 value;

  struct
  {
    unsigned mantissa3:32;
    unsigned mantissa2:32;
    unsigned mantissa1:32;
    unsigned mantissa0:16;
    unsigned exponent:15;
    unsigned negative:1;
  } ieee;

  struct
  {
    uint64_t low;
    uint64_t high;
  } words64;

  struct
  {
    uint32_t w3;
    uint32_t w2;
    uint32_t w1;
    uint32_t w0;
  } words32;

  struct
  {
    unsigned mantissa3:32;
    unsigned mantissa2:32;
    unsigned mantissa1:32;
    unsigned mantissa0:15;
    unsigned quiet_nan:1;
    unsigned exponent:15;
    unsigned negative:1;
  } ieee_nan;

} mcs_ieee854_float128;

/* Get two 64 bit ints from a long double.  */
#define MCS_GET_FLT128_WORDS64(ix0,ix1,d)  \
do {                                   \
  mcs_ieee854_float128 u;                  \
  u.value = (d);                       \
  (ix0) = u.words64.high;              \
  (ix1) = u.words64.low;               \
} while (0)

/* Set a long double from two 64 bit ints.  */
#define MCS_SET_FLT128_WORDS64(d,ix0,ix1)  \
do {                                   \
  mcs_ieee854_float128 u;                  \
  u.words64.high = (ix0);              \
  u.words64.low = (ix1);               \
  (d) = u.value;                       \
} while (0)

class TSInt128;
class TFloat128;
using int128_t = __int128;

static const __float128 mcs_fl_one = 1.0, mcs_fl_Zero[] = {0.0, -0.0,};

template<typename T>
class numeric_limits { };
// Copy from boost::multiprecision::float128
template<> class numeric_limits<__float128> {
  public:
    static constexpr bool is_specialized = true;
    static constexpr __float128 max()
    {
      return mcs_ieee854_float128{ .ieee = {0xffffffff,
                                            0xffffffff,
                                            0xffffffff,
                                            0xffff,
                                            0x7ffe,
                                            0x0}}.value;
    }
    static constexpr __float128 min()
    {
      return mcs_ieee854_float128{ .ieee = {0x0,
                                            0x0,
                                            0x0,
                                            0x0,
                                            0x1,
                                            0x0}}.value;
    }
    static __float128 denorm_min()
    {
      return mcs_ieee854_float128{ .ieee = {0x1,
                                            0x0,
                                            0x0,
                                            0x0,
                                            0x0,
                                            0x0}}.value;
    }
    static __float128 lowest() { return -max(); }
    static constexpr int digits = 113;
    static constexpr int digits10 = 33;
    static constexpr int max_digits10 = 36;
    static constexpr bool is_signed = true;
    static constexpr bool is_integer = false;
    static constexpr bool is_exact = false;
    static constexpr int radix = 2;
    static __float128 round_error() { return 0.5; }
    static constexpr int min_exponent = -16381;
    static constexpr int min_exponent10 = min_exponent * 301L / 1000L;
    static constexpr int max_exponent = 16384;
    static constexpr int max_exponent10 = max_exponent * 301L / 1000L;
    static constexpr bool has_infinity = true;
    static constexpr bool has_quiet_NaN = true;
    static __float128 quiet_NaN() { return 1.0 / 0.0; }
    static constexpr bool has_signaling_NaN = false;
    static constexpr bool has_denorm_loss = true;
    static __float128 infinity() { return 1.0 / 0.0; }
    static __float128 signaling_NaN() { return 0; }
    static constexpr bool is_iec559 = true;
    static constexpr bool is_bounded = false;
    static constexpr bool is_modulo = false;
    static constexpr bool traps = false;
    static constexpr bool tinyness_before = false;
};

// Type defined integral types
// for templates
template <typename T>
struct get_integral_type {
  typedef T type;
};

template<>
struct get_integral_type<TFloat128>{
  typedef __float128 type;
};

template<>
struct get_integral_type<TSInt128>{
  typedef int128_t type;
};

class TFloat128
{
  public:
    static constexpr int128_t minInt128 = int128_t(0x8000000000000000LL) << 64;
    static constexpr int128_t maxInt128 = (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;

    static constexpr uint16_t MAXLENGTH16BYTES = 42;
    //  A variety of ctors for aligned and unaligned arguments
    TFloat128(): value(0) { }

    //    aligned argument
    TFloat128(const __float128& x) { value = x; }
    TFloat128(const int128_t& x) { value = static_cast<__float128>(x); }

    // fmodq(x,y) taken from libquadmath
    // Return x mod y in exact arithmetic
    // Method: shift and subtract
    static __float128 fmodq (__float128& x, __float128& y)
    {
      int64_t n,hx,hy,hz,ix,iy,sx,i;
      uint64_t lx,ly,lz;

      MCS_GET_FLT128_WORDS64(hx,lx,x);
      MCS_GET_FLT128_WORDS64(hy,ly,y);
      sx = hx&0x8000000000000000ULL;		/* sign of x */
      hx ^=sx;				/* |x| */
      hy &= 0x7fffffffffffffffLL;		/* |y| */

        /* purge off exception values */
      if((hy|ly)==0||(hx>=0x7fff000000000000LL)|| /* y=0,or x not finite */
        ((hy|((ly|-ly)>>63))>0x7fff000000000000LL))	/* or y is NaN */
          return (x*y)/(x*y);
      if(hx<=hy) {
          if((hx<hy)||(lx<ly)) return x;	/* |x|<|y| return x */
          if(lx==ly)
        return mcs_fl_Zero[(uint64_t)sx>>63];	/* |x|=|y| return x*0*/
      }

        /* determine ix = ilogb(x) */
      if(hx<0x0001000000000000LL) {	/* subnormal x */
          if(hx==0) {
        for (ix = -16431, i=lx; i>0; i<<=1) ix -=1;
          } else {
        for (ix = -16382, i=hx<<15; i>0; i<<=1) ix -=1;
          }
      } else ix = (hx>>48)-0x3fff;

        /* determine iy = ilogb(y) */
      if(hy<0x0001000000000000LL) {	/* subnormal y */
          if(hy==0) {
        for (iy = -16431, i=ly; i>0; i<<=1) iy -=1;
          } else {
        for (iy = -16382, i=hy<<15; i>0; i<<=1) iy -=1;
          }
      } else iy = (hy>>48)-0x3fff;

        /* set up {hx,lx}, {hy,ly} and align y to x */
      if(ix >= -16382)
          hx = 0x0001000000000000LL|(0x0000ffffffffffffLL&hx);
      else {		/* subnormal x, shift x to normal */
          n = -16382-ix;
          if(n<=63) {
        hx = (hx<<n)|(lx>>(64-n));
        lx <<= n;
          } else {
        hx = lx<<(n-64);
        lx = 0;
          }
      }
      if(iy >= -16382)
          hy = 0x0001000000000000LL|(0x0000ffffffffffffLL&hy);
      else {		/* subnormal y, shift y to normal */
          n = -16382-iy;
          if(n<=63) {
        hy = (hy<<n)|(ly>>(64-n));
        ly <<= n;
          } else {
        hy = ly<<(n-64);
        ly = 0;
          }
      }

        /* fix point fmod */
      n = ix - iy;
      while(n--) {
          hz=hx-hy;lz=lx-ly; if(lx<ly) hz -= 1;
          if(hz<0){hx = hx+hx+(lx>>63); lx = lx+lx;}
          else {
        if((hz|lz)==0)		/* return sign(x)*0 */
            return mcs_fl_Zero[(uint64_t)sx>>63];
        hx = hz+hz+(lz>>63); lx = lz+lz;
          }
      }
      hz=hx-hy;lz=lx-ly; if(lx<ly) hz -= 1;
      if(hz>=0) {hx=hz;lx=lz;}

        /* convert back to floating value and restore the sign */
      if((hx|lx)==0)			/* return sign(x)*0 */
          return mcs_fl_Zero[(uint64_t)sx>>63];
      while(hx<0x0001000000000000LL) {	/* normalize x */
          hx = hx+hx+(lx>>63); lx = lx+lx;
          iy -= 1;
      }
      if(iy>= -16382) {	/* normalize output */
          hx = ((hx-0x0001000000000000LL)|((iy+16383)<<48));
          MCS_SET_FLT128_WORDS64(x,hx|sx,lx);
      } else {		/* subnormal output */
          n = -16382 - iy;
          if(n<=48) {
        lx = (lx>>n)|((uint64_t)hx<<(64-n));
        hx >>= n;
          } else if (n<=63) {
        lx = (hx<<(64-n))|(lx>>n); hx = sx;
          } else {
        lx = hx>>(n-64); hx = sx;
          }
          MCS_SET_FLT128_WORDS64(x,hx|sx,lx);
          x *= mcs_fl_one;		/* create necessary signal */
      }
      return x;		/* exact output */
    }

    // The f() returns __float128 power p
    // taken from boost::multiprecision
    static inline __float128 pown(const __float128& x, const int p)
    {
      const bool isneg  = (x < 0);
      const bool isnan  = (x != x);
      const bool isinf  = ((!isneg) ? bool(+x > (datatypes::numeric_limits<__float128>::max)())
                                    : bool(-x > (datatypes::numeric_limits<__float128>::max)()));

      if(isnan) { return x; }

      if(isinf) { return datatypes::numeric_limits<__float128>::quiet_NaN(); }

      const bool       x_is_neg = (x < 0);
      const __float128 abs_x    = (x_is_neg ? -x : x);

      if(p < static_cast<int>(0))
      {
        if(abs_x < (datatypes::numeric_limits<__float128>::min)())
        {
          return (x_is_neg ? -datatypes::numeric_limits<__float128>::infinity()
                           : +datatypes::numeric_limits<__float128>::infinity());
        }
        else
        {
          return __float128(1) / pown(x, static_cast<int>(-p));
        }
      }

      if(p == static_cast<int>(0))
      {
        return __float128(1);
      }
      else
      {
        if(p == static_cast<int>(1)) { return x; }

        if(abs_x > (datatypes::numeric_limits<__float128>::max)())
        {
          return (x_is_neg ? -datatypes::numeric_limits<__float128>::infinity()
                           : +datatypes::numeric_limits<__float128>::infinity());
        }

        if     (p == static_cast<int>(2)) { return  (x * x); }
        else if(p == static_cast<int>(3)) { return ((x * x) * x); }
        else if(p == static_cast<int>(4)) { const __float128 x2 = (x * x); return (x2 * x2); }
        else
        {
          // The variable xn stores the binary powers of x.
          __float128 result(((p % int(2)) != int(0)) ? x : __float128(1));
          __float128 xn    (x);

          int p2 = p;

          while(int(p2 /= 2) != int(0))
          {
            // Square xn for each binary power.
            xn *= xn;

            const bool has_binary_power = (int(p2 % int(2)) != int(0));

            if(has_binary_power)
            {
              // Multiply the result with each binary power contained in the exponent.
              result *= xn;
            }
          }

          return result;
        }
      }
    }

    // fromString conversion for __float128
    // algo is taken from
    // boost/math/cstdfloat/cstdfloat_iostream.hpp:convert_from_string()
    static __float128 fromString(const std::string& str)
    {
      __float128 value = 0;
      const char* p = str.c_str();

      if((p == static_cast<const char*>(0U)) || (*p == static_cast<char>(0)))
      {
        return value;
      }

      bool is_neg       = false;
      bool is_neg_expon = false;

      constexpr int ten = 10;

      int expon       = 0;
      int digits_seen = 0;

      constexpr int max_digits10 = datatypes::numeric_limits<__float128>::max_digits10 + 1;

      if(*p == static_cast<char>('+'))
      {
        ++p;
      }
      else if(*p == static_cast<char>('-'))
      {
        is_neg = true;
        ++p;
      }

      const bool isnan = ((std::strcmp(p, "nan") == 0) || (std::strcmp(p, "NaN") == 0) || (std::strcmp(p, "NAN") == 0));

      if(isnan)
      {
        value = datatypes::numeric_limits<__float128>::infinity();
        if (is_neg)
        {
          value = -value;
        }
        return value;
      }

      const bool isinf = ((std::strcmp(p, "inf") == 0) || (std::strcmp(p, "Inf") == 0) || (std::strcmp(p, "INF") == 0));

      if(isinf)
      {
        value = datatypes::numeric_limits<__float128>::infinity();
        if (is_neg)
        {
          value = -value;
        }
        return value;
      }

      // Grab all the leading digits before the decimal point.
      while(std::isdigit(*p))
      {
        value *= ten;
        value += static_cast<int>(*p - '0');
        ++p;
        ++digits_seen;
      }

      if(*p == static_cast<char>('.'))
      {
        // Grab everything after the point, stop when we've seen
        // enough digits, even if there are actually more available.

        ++p;

        while(std::isdigit(*p))
        {
          value *= ten;
          value += static_cast<int>(*p - '0');
          ++p;
          --expon;

          if(++digits_seen > max_digits10)
          {
            break;
          }
        }

        while(std::isdigit(*p))
        {
          ++p;
        }
      }

      // Parse the exponent.
      if((*p == static_cast<char>('e')) || (*p == static_cast<char>('E')))
      {
        ++p;

        if(*p == static_cast<char>('+'))
        {
          ++p;
        }
        else if(*p == static_cast<char>('-'))
        {
          is_neg_expon = true;
          ++p;
        }

        int e2 = 0;

        while(std::isdigit(*p))
        {
          e2 *= 10;
          e2 += (*p - '0');
          ++p;
        }

        if(is_neg_expon)
        {
          e2 = -e2;
        }

        expon += e2;
      }

      if(expon)
      {
        // Scale by 10^expon. Note that 10^expon can be outside the range
        // of our number type, even though the result is within range.
        // If that looks likely, then split the calculation in two parts.
        __float128 t;
        t = ten;

        if(expon > (datatypes::numeric_limits<__float128>::min_exponent10 + 2))
        {
          t = TFloat128::pown(t, expon);
          value *= t;
        }
        else
        {
          t = TFloat128::pown(t, (expon + digits_seen + 1));
          value *= t;
          t = ten;
          t = TFloat128::pown(t, (-digits_seen - 1));
          value *= t;
        }
      }

      if(is_neg)
      {
        value = -value;
      }
      return value;
    }

    //    Method returns max length of a string representation
    static constexpr uint8_t maxLength()
    {
      return TFloat128::MAXLENGTH16BYTES;
    }

    inline int128_t toTSInt128() const
    {
      if (value > static_cast<__float128>(maxInt128))
        return maxInt128;
      else if (value < static_cast<__float128>(minInt128))
        return minInt128;

      return static_cast<int128_t>(value);
    }

    inline operator int128_t() const
    {
      return toTSInt128();
    }

    inline operator double() const
    {
      return toDouble();
    }

    inline double toDouble() const
    {
        if (value > static_cast<__float128>(DBL_MAX))
            return DBL_MAX;
        else if (value < -static_cast<__float128>(DBL_MAX))
            return -DBL_MAX;

        return static_cast<double>(value);
    }

    inline operator long double() const
    {
      return toLongDouble();
    }

    inline operator float() const
    {
      return toFloat();
    }

    inline float toFloat() const
    {
        if (value > static_cast<__float128>(FLT_MAX))
            return FLT_MAX;
        else if (value < -static_cast<__float128>(FLT_MAX))
            return -FLT_MAX;

        return static_cast<float>(value);
    }

    inline int64_t toTSInt64() const
    {
      if (value > static_cast<__float128>(INT64_MAX))
          return INT64_MAX;
      else if (value < static_cast<__float128>(INT64_MIN))
          return INT64_MIN;

      return static_cast<int64_t>(value);
    }

    inline operator int64_t() const
    {
      return toTSInt64();
    }

    inline uint64_t toTUInt64() const
    {
      if (value > static_cast<__float128>(UINT64_MAX))
          return UINT64_MAX;
      else if (value < 0)
          return 0;

      return static_cast<uint64_t>(value);
    }

    inline operator uint64_t() const
    {
      return toTUInt64();
    }

    inline TFloat128 operator+(const TFloat128& rhs) const
    {
      return TFloat128(value + rhs.value);
    }

    inline long double toLongDouble() const
    {
        if (value > static_cast<__float128>(LDBL_MAX))
            return LDBL_MAX;
        else if (value < -static_cast<__float128>(LDBL_MAX))
            return -LDBL_MAX;

        return static_cast<long double>(value);
    }
  private:
    __float128 value;
};

} //end of namespace

#endif // MCS_TSFLOAT128_H_INCLUDED
// vim:ts=2 sw=2:

