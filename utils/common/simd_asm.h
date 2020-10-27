/* Copyright (C) 2020 MariaDB Corporation

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
   MA 02110-1301, USA. */
#ifndef H_SIMD_ASM
#define H_SIMD_ASM

// Inline asm has three argument lists: output, input and clobber list

#if defined(__GNUC__) && (__GNUC___ > 7)
    #define MACRO_VALUE_PTR_128(dst, \
                                dst_restrictions, \
                                src, \
                                src_restrictions, \
                                clobb) \
        ::memcpy((dst), (src), sizeof(__int128));
    #define MACRO_PTR_PTR_128(dst, \
                              dst_restrictions, \
                              src, \
                              src_restrictions, \
                              clobb) \
        ::memcpy((dst), (src), sizeof(__int128));

#else
    #define MACRO_VALUE_PTR_128(dst, \
                                dst_restrictions, \
                                src, \
                                src_restrictions, \
                                clobb) \
        __asm__ volatile("movups %1,%0" \
            :dst_restrictions ( *(dst) ) \
            :src_restrictions ( (src) ) \
            :clobb \
        );
    #define MACRO_PTR_PTR_128(dst, \
                              dst_restrictions, \
                              src, \
                              src_restrictions, \
                              clobb) \
        __asm__ volatile("movdqu %1,%%xmm0;" \
                         "movups %%xmm0,%0;" \
            :dst_restrictions ( *(dst) ) \
            :src_restrictions ( *(src) ) \
            :"memory", clobb \
        );

#endif
namespace common
{

template <typename D, typename S>
inline void assign128BitPtrPtr(D* dst, S* src)
{
     MACRO_PTR_PTR_128(dst, "=m", src, "m", "xmm0")
}

template <typename D, typename S>
inline void assign128BitPtrPtr(D* dst, const S* src)
{
    MACRO_PTR_PTR_128(dst, "=m", src, "m", "xmm0")
} 

}

#endif
