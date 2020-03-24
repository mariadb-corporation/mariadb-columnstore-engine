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

#include <functional>
#include <string>

#include "mcs_decimal.h"
#include "treenode.h"
#include "exceptclasses.h"

namespace datatypes
{

    struct lldiv_t_128
    {
        int128_t quot;
        int128_t rem;
        lldiv_t_128() : quot(0), rem(0) {}
    };

    inline lldiv_t_128 lldiv128(const int128_t& dividend, const int128_t& divisor)
    {
        lldiv_t_128 res;

        if (UNLIKELY(divisor == 0) || UNLIKELY(dividend == 0))
            return res;

        res.quot = dividend / divisor;
        res.rem = dividend % divisor;

        return res;
    }

    template<typename BinaryOperation,
        typename OpOverflowCheck,
        typename MultiplicationOverflowCheck>
    void execute(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r,
        execplan::IDB_Decimal& result,
        BinaryOperation op,
        OpOverflowCheck opOverflowCheck,
        MultiplicationOverflowCheck mulOverflowCheck)
    {
        int128_t lValue = Decimal::isWideDecimalType(l.precision)
            ? l.s128Value : l.value;
        int128_t rValue = Decimal::isWideDecimalType(l.precision)
            ? r.s128Value : r.value;

        if (result.scale == l.scale && result.scale == r.scale)
        {
            opOverflowCheck(lValue, rValue);
            result.s128Value = op(lValue, rValue);
            return;
        }

        if (result.scale > l.scale)
        {
            int128_t scaleMultiplier;
            getScaleDivisor(scaleMultiplier, result.scale - l.scale);
            mulOverflowCheck(lValue, scaleMultiplier);
            lValue *= scaleMultiplier;
        }
        else if (result.scale < l.scale)
        {
            int128_t scaleMultiplier;
            getScaleDivisor(scaleMultiplier, l.scale - result.scale);
            lValue /= scaleMultiplier;
        }

        if (result.scale > r.scale)
        {
            int128_t scaleMultiplier;
            getScaleDivisor(scaleMultiplier, result.scale - r.scale);
            mulOverflowCheck(rValue, scaleMultiplier);
            rValue *= scaleMultiplier;
        }
        else if (result.scale < r.scale)
        {
            int128_t scaleMultiplier;
            getScaleDivisor(scaleMultiplier, r.scale - result.scale);
            rValue /= scaleMultiplier;
        }

        // We assume there is no way that lValue or rValue calculations
        // give an overflow and this is an incorrect assumption.
        opOverflowCheck(lValue, rValue);

        result.s128Value = op(lValue, rValue);
    }

    std::string Decimal::toString(execplan::IDB_Decimal& value)
    {
        char buf[utils::MAXLENGTH16BYTES];
        dataconvert::DataConvert::decimalToString(&value.s128Value,
            value.scale, buf, sizeof(buf),
            execplan::CalpontSystemCatalog::DECIMAL);
        return std::string(buf);
    }

    int Decimal::compare(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r)
    {
        int128_t divisorL, divisorR;
        getScaleDivisor(divisorL, l.scale);
        getScaleDivisor(divisorR, r.scale);

        lldiv_t_128 d1 = lldiv128(l.s128Value, divisorL);
        lldiv_t_128 d2 = lldiv128(r.s128Value, divisorR);

        int ret = 0;

        if (d1.quot > d2.quot)
        {
            ret = 1;
        }
        else if (d1.quot < d2.quot)
        {
            ret = -1;
        }
        else
        {
            // rem carries the value's sign, but needs to be normalized.
            int64_t s = l.scale - r.scale;
            int128_t divisor;
            getScaleDivisor(divisor, abs(s));

            if (s < 0)
            {
                if ((d1.rem * divisor) > d2.rem)
                    ret = 1;
                else if ((d1.rem * divisor) < d2.rem)
                    ret = -1;
            }
            else
            {
                if (d1.rem > (d2.rem * divisor))
                    ret = 1;
                else if (d1.rem < (d2.rem * divisor))
                    ret = -1;
            }
        }

        return ret;
    }

    // no overflow check
    template<>
    void Decimal::addition<int128_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        std::plus<int128_t> add;
        NoOverflowCheck noOverflowCheck;
        execute(l, r, result, add, noOverflowCheck, noOverflowCheck);
    }

    template
    void Decimal::addition<int128_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result);

    // with overflow check
    template<>
    void Decimal::addition<int128_t, true>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        std::plus<int128_t> add;
        AdditionOverflowCheck overflowCheck;
        MultiplicationOverflowCheck mulOverflowCheck;
        execute(l, r, result, add, overflowCheck, mulOverflowCheck);
    }

    template
    void Decimal::addition<int128_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result);

    template<>
    void Decimal::addition<int64_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        if (result.scale == l.scale && result.scale == r.scale)
        {
            result.value = l.value + r.value;
            return;
        }

        int64_t lValue = 0, rValue = 0;

        if (result.scale >= l.scale)
            lValue = l.value * mcs_pow_10[result.scale - l.scale];
        else
            lValue = (int64_t)(l.value > 0 ?
                                  (double)l.value / mcs_pow_10[l.scale - result.scale] + 0.5 :
                                  (double)l.value / mcs_pow_10[l.scale - result.scale] - 0.5);

        if (result.scale >= r.scale)
            rValue = r.value * mcs_pow_10[result.scale - r.scale];
        else
            rValue = (int64_t)(r.value > 0 ?
                                  (double)r.value / mcs_pow_10[r.scale - result.scale] + 0.5 :
                                  (double)r.value / mcs_pow_10[r.scale - result.scale] - 0.5);

        result.value = lValue + rValue;
    }
    template
    void Decimal::addition<int64_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result);

    template<>
    void Decimal::addition<int64_t, true>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        throw logging::NotImplementedExcept("Decimal::addition<int64>");
    }

    template<>
    void Decimal::division<int128_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        std::divides<int128_t> division;
        NoOverflowCheck noOverflowCheck;
        execute(l, r, result, division, noOverflowCheck, noOverflowCheck);
    }

    template
    void Decimal::division<int128_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result);

    // With overflow check
    template<>
    void Decimal::division<int128_t, true>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        std::divides<int128_t> division;
        DivisionOverflowCheck overflowCheck;
        MultiplicationOverflowCheck mulOverflowCheck;
        execute(l, r, result, division, overflowCheck, mulOverflowCheck);
    }

    // We rely on the zero check from ArithmeticOperator::execute
    template<>
    void Decimal::division<int64_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        if (result.scale >= l.scale - r.scale)
            result.value = (int64_t)(( (l.value > 0 && r.value > 0) 
                || (l.value < 0 && r.value < 0) ?
                    (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] + 0.5 :
                    (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] - 0.5));
            else
                result.value = (int64_t)(( (l.value > 0 && r.value > 0)
                    || (l.value < 0 && r.value < 0) ?
                        (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] + 0.5 :
                        (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] - 0.5));

    }

    template
    void Decimal::division<int64_t, false>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result);

    template<>
    void Decimal::division<int64_t, true>(const execplan::IDB_Decimal& l,
        const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
    {
        throw logging::NotImplementedExcept("Decimal::division<int64>");
    }

} // end of namespace
