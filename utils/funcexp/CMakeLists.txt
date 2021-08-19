
include_directories( ${ENGINE_COMMON_INCLUDES} )


########### next target ###############
#    func_decode_oracle.cpp

set(funcexp_LIB_SRCS
    functor.cpp
    funcexp.cpp
    funcexpwrapper.cpp
    func_abs.cpp
    func_add_time.cpp
    func_ascii.cpp
    func_between.cpp
    func_bitwise.cpp
    func_case.cpp
    func_cast.cpp
    func_ceil.cpp
    func_char.cpp
    func_char_length.cpp
    func_coalesce.cpp
    func_concat.cpp
    func_concat_oracle.cpp
    func_concat_ws.cpp
    func_conv.cpp
    func_convert_tz.cpp
    func_crc32.cpp
    func_date.cpp
    func_date_add.cpp
    func_date_format.cpp
    func_day.cpp
    func_dayname.cpp
    func_dayofweek.cpp
    func_dayofyear.cpp
    func_decode.cpp
    func_decode_oracle.cpp
    func_div.cpp
    func_elt.cpp
    func_encode.cpp
    func_exp.cpp
    func_extract.cpp
    func_find_in_set.cpp
    func_floor.cpp
    func_from_days.cpp
    func_from_unixtime.cpp
    func_get_format.cpp
    func_greatest.cpp
    func_hex.cpp
    func_hour.cpp
    func_idbpartition.cpp
    func_if.cpp
    func_ifnull.cpp
    func_in.cpp
    func_inet_aton.cpp
    func_inet_ntoa.cpp
    func_insert.cpp
    func_instr.cpp
    func_isnull.cpp
    func_last_day.cpp
    func_lcase.cpp
    func_least.cpp
    func_left.cpp
    func_length.cpp
    func_lpad.cpp
    func_ltrim.cpp
    func_ltrim_oracle.cpp
    func_makedate.cpp
    func_maketime.cpp
    func_math.cpp
    func_microsecond.cpp
    func_md5.cpp
    func_minute.cpp
    func_mod.cpp
    func_month.cpp
    func_monthname.cpp
    func_nullif.cpp
    func_pow.cpp
    func_period_add.cpp
    func_period_diff.cpp
    func_quote.cpp
    func_quarter.cpp
    func_rand.cpp
    func_regexp.cpp
    func_repeat.cpp
    func_replace.cpp
    func_replace_oracle.cpp
    func_reverse.cpp
    func_right.cpp
    func_round.cpp
    func_rpad.cpp
    func_rtrim.cpp
    func_rtrim_oracle.cpp
    func_second.cpp
    func_sec_to_time.cpp
    func_sha.cpp
    func_sign.cpp
    func_space.cpp
    func_str_to_date.cpp
    func_strcmp.cpp
    func_substr.cpp
    func_substring_index.cpp
    func_sysdate.cpp
    func_time.cpp
    func_time_format.cpp
    func_time_to_sec.cpp
    func_timediff.cpp
    func_timestampdiff.cpp
    func_to_days.cpp
    func_trim.cpp
    func_trim_oracle.cpp
    func_truncate.cpp
    func_ucase.cpp
    func_unhex.cpp
    func_unix_timestamp.cpp
    func_week.cpp
    func_weekday.cpp
    func_year.cpp
    func_yearweek.cpp
    sql_crypt.cpp)

add_library(funcexp SHARED ${funcexp_LIB_SRCS})

add_dependencies(funcexp loggingcpp)

target_link_libraries(funcexp ${NETSNMP_LIBRARIES})

install(TARGETS funcexp DESTINATION ${ENGINE_LIBDIR} COMPONENT columnstore-engine)

