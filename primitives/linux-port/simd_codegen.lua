-- Generate SIMD processing code for ColumnFilter

print [[
template <SIMDSET s, ENUM_KIND KIND, int COL_WIDTH, uint8_t COP, uint8_t BOP>
processFilterElement(...)
{
    // Default implementation just calls the scalar code
    scalarProcessing<KIND, COL_WIDTH, COP, BOP>(...);
}
]]

local function_template = [[
template <>
processFilterElement<SIMD_%s, KIND_%s, %d, COMPARE_%s, BOP_%s>(...)
{
    for (int i=0; i<size; i+=4)
    {
        auto cmp = %s;
        %s;
    }
}
]]


function main()
    for _, simdset in ipairs{"sse42", "avx2"} do
        for _, datatype in ipairs{"i64"} do
            for _, cop in ipairs{"eq", "ne", "gt", "lt", "le", "ge"} do
                for _, bop in ipairs{"and", "or", "xor"} do
                    generate(simdset, datatype, cop, bop)
                end
            end
        end
    end
end


function generate(simdset, datatype, cop, bop)
    print("//", simdset, datatype, cop, bop)  -- for debugging/clarity

    -- put here all sse/avx differences
    PREFIX = (simdset=="sse42" and "_mm_" or "_mm256_")
    SUFFIX = (simdset=="sse42" and "_si128" or "_si256")

    -- replace unsupported COPs with eq/gt + some code massaging: https://fgiesen.wordpress.com/2016/04/03/sse-mind-the-gap/
    cmp     = cop    -- should finally became "eq" or "gt", the only comparisons supported by SSE/AVX
    negate  = false  -- do we need to negate result?
    reverse = false  -- do we need to reverse operands?

    if     cop=="ne" then cmp="eq"; negate = true
    elseif cop=="lt" then cmp="gt"; reverse = true
    elseif cop=="le" then cmp="gt"; negate = true
    elseif cop=="ge" then cmp="gt"; reverse = true; negate = true
    end

    -- define all intrinsics that will be used
    load_op = PREFIX.."load"..SUFFIX
    save_op = PREFIX.."store"..SUFFIX
    cmp_op = PREFIX.."cmp"..cmp.."_ep"..datatype
    filter_op = PREFIX..bop..SUFFIX

    -- the compare expression
    mem = load_op.."( &data[i])"
    operands = reverse and "value, "..mem or mem..", value"
    cmp_expression = cmp_op.."( "..operands..")"
    if negate then cmp_expression = "~ "..cmp_expression end

    -- the filter update operation
    filter_mem = load_op.."( &filter[i])"
    filter_expression = filter_op.."( "..filter_mem..", cmp)"
    filter_command = save_op.."( &filter[i], "..filter_expression..")"

    kind = "DEFAULT"
    col_width = datatype:sub(2) / 8
    print(function_template:format(simdset:upper(), kind, col_width, cop:upper(), bop:upper(), -- template<> params
                                   cmp_expression, filter_command))
end


main()
