#include "simplecolumn.h"
#include "constantfilter.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"
#include "existsfilter.h"

namespace execplan
{
auto reference_Query_1 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("or"),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` + `test`.`t2`.`pos` > 15000"), nullptr, nullptr),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` + `test`.`t2`.`pos` < 1000"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` = `test`.`t2`.`id`"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_11 = new ParseTree(
    new LogicOperator("and"), new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
    new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_15 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(new LogicOperator("or"),
                  new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
                  new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
    new ParseTree(new ExistsFilter(boost::shared_ptr<CalpontSelectExecutionPlan>(), 0, 1), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_16 = new ParseTree(
    new LogicOperator("and"), new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
    new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_17 = new ParseTree(
    new LogicOperator("and"), new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
    new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_18 = new ParseTree(
    new LogicOperator("and"), new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
    new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_19 = new ParseTree(
    new LogicOperator("and"), new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr),
    new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_20 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("or"),
        new ParseTree(new LogicOperator("and"),
                      new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                      new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_21 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("or"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr),
            new ParseTree(
                new LogicOperator("and"),
                new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr))),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_22 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr),
            new ParseTree(
                new LogicOperator("and"),
                new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr))),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_23 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr),
            new ParseTree(
                new LogicOperator("and"),
                new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr))),
        new ParseTree(new SimpleFilter("5000 < `test`.`t1`.`pos`"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_27 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("and"),
            new ParseTree(
                new LogicOperator("or"),
                new ParseTree(
                    new LogicOperator("or"),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr)),
                new ParseTree(
                    new LogicOperator("and"),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr))),
            new ParseTree(new SimpleFilter("`test`.`t1`.`rid` > 20"), nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_Query_28 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("and"),
            new ParseTree(
                new LogicOperator("and"),
                new ParseTree(
                    new LogicOperator("and"),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'"), nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'"), nullptr, nullptr)),
                new ParseTree(new SimpleFilter("`test`.`t1`.`rid` > 20"), nullptr, nullptr)),
            new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000"), nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'"), nullptr, nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30"), nullptr, nullptr));
}

namespace execplan
{
auto reference_TPCH_19 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("and"),
            new ParseTree(
                new LogicOperator("and"),
                new ParseTree(
                    new LogicOperator("or"),
                    new ParseTree(
                        new LogicOperator("or"),
                        new ParseTree(
                            new LogicOperator("and"),
                            new ParseTree(
                                new LogicOperator("and"),
                                new ParseTree(
                                    new LogicOperator("and"),
                                    new ParseTree(
                                        new LogicOperator("and"),
                                        new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_size` <= 15"),
                                                      nullptr, nullptr),
                                        new ParseTree(
                                            new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` <= 30"), nullptr,
                                            nullptr)),
                                    new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 20"),
                                                  nullptr, nullptr)),
                                new ParseTree(
                                    new ConstantFilter(boost::shared_ptr<Operator>(new LogicOperator("or")),
                                                       ConstantFilter::FilterList{
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'LG PKG'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'LG PACK'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'LG BOX'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'LG CASE'"))},
                                                       boost::shared_ptr<ReturnedColumn>(new SimpleColumn(
                                                           "tpch", "PART", "p_container", 0, 1, false)),
                                                       ""),
                                    nullptr, nullptr)),
                            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#34'"), nullptr,
                                          nullptr)),
                        new ParseTree(
                            new LogicOperator("and"),
                            new ParseTree(
                                new LogicOperator("and"),
                                new ParseTree(
                                    new LogicOperator("and"),
                                    new ParseTree(
                                        new LogicOperator("and"),
                                        new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_size` <= 10"),
                                                      nullptr, nullptr),
                                        new ParseTree(
                                            new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` <= 20"), nullptr,
                                            nullptr)),
                                    new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 10"),
                                                  nullptr, nullptr)),
                                new ParseTree(
                                    new ConstantFilter(boost::shared_ptr<Operator>(new LogicOperator("or")),
                                                       ConstantFilter::FilterList{
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'MED PACK'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'MED PKG'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'MED BOX'")),
                                                           boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                               "`tpch`.`PART`.`p_container` = 'MED BAG'"))},
                                                       boost::shared_ptr<ReturnedColumn>(new SimpleColumn(
                                                           "tpch", "PART", "p_container", 0, 1, false)),
                                                       ""),
                                    nullptr, nullptr)),
                            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#23'"), nullptr,
                                          nullptr))),
                    new ParseTree(
                        new LogicOperator("and"),
                        new ParseTree(
                            new LogicOperator("and"),
                            new ParseTree(
                                new LogicOperator("and"),
                                new ParseTree(
                                    new LogicOperator("and"),
                                    new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_size` <= 5"), nullptr,
                                                  nullptr),
                                    new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` <= 11"),
                                                  nullptr, nullptr)),
                                new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 1"),
                                              nullptr, nullptr)),
                            new ParseTree(
                                new ConstantFilter(boost::shared_ptr<Operator>(new LogicOperator("or")),
                                                   ConstantFilter::FilterList{
                                                       boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                           "`tpch`.`PART`.`p_container` = 'SM PKG'")),
                                                       boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                           "`tpch`.`PART`.`p_container` = 'SM PACK'")),
                                                       boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                           "`tpch`.`PART`.`p_container` = 'SM BOX'")),
                                                       boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                           "`tpch`.`PART`.`p_container` = 'SM CASE'"))},
                                                   boost::shared_ptr<ReturnedColumn>(new SimpleColumn(
                                                       "tpch", "PART", "p_container", 0, 1, false)),
                                                   ""),
                                nullptr, nullptr)),
                        new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#12'"), nullptr,
                                      nullptr))),
                new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_size` >= 1"), nullptr, nullptr)),
            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_partkey` = `tpch`.`LINEITEM`.`l_partkey`"),
                          nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_shipinstruct` = 'DELIVER IN PERSON'"), nullptr,
                      nullptr)),
    new ParseTree(
        new ConstantFilter(boost::shared_ptr<Operator>(new LogicOperator("or")),
                           ConstantFilter::FilterList{boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                          "`tpch`.`LINEITEM`.`l_shipmode` = 'AIR REG'")),
                                                      boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                          "`tpch`.`LINEITEM`.`l_shipmode` = 'AIR'"))},
                           boost::shared_ptr<ReturnedColumn>(
                               new SimpleColumn("tpch", "LINEITEM", "l_shipmode", 0, 1, false)),
                           ""),
        nullptr, nullptr));
}
