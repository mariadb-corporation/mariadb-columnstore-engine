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
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` + `test`.`t2`.`pos` > 15000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                      nullptr, nullptr),
        new ParseTree(new SimpleFilter("`test`.`t1`.`pos` + `test`.`t2`.`pos` < 1000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
                      nullptr)),
    new ParseTree(new SimpleFilter("`test`.`t1`.`id` = `test`.`t2`.`id`",
                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                  nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_11 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
        nullptr, nullptr));
}

#include "existsfilter.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_15 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(new LogicOperator("or"),
                  new ParseTree(new SimpleFilter("`test`.`t1`.`id` < 30",
                                                 SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                nullptr, nullptr),
                  new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000",
                                                 SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                nullptr, nullptr)),
    new ParseTree(new ExistsFilter(boost::shared_ptr<CalpontSelectExecutionPlan>(), 0, 1), nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_16 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
        nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_17 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
        nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_18 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
        nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_19 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
        nullptr, nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_20 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("or"),
        new ParseTree(new LogicOperator("and"),
                      new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                     SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                    nullptr, nullptr),
                      new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                     SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                    nullptr, nullptr)),
        new ParseTree(
            new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
            nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_21 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("or"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                          nullptr, nullptr),
            new ParseTree(new LogicOperator("and"),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr))),
        new ParseTree(
            new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
            nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_22 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                          nullptr, nullptr),
            new ParseTree(new LogicOperator("and"),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr))),
        new ParseTree(
            new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
            nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

namespace execplan
{
auto reference_Query_23 = new ParseTree(
    new LogicOperator("and"),
    new ParseTree(
        new LogicOperator("and"),
        new ParseTree(
            new LogicOperator("or"),
            new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                          nullptr, nullptr),
            new ParseTree(new LogicOperator("and"),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr),
                          new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr))),
        new ParseTree(
            new SimpleFilter("5000 < `test`.`t1`.`pos`", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
            nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

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
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr)),
                new ParseTree(
                    new LogicOperator("and"),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr))),
            new ParseTree(
                new SimpleFilter("`test`.`t1`.`rid` > 20", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                nullptr, nullptr)),
        new ParseTree(
            new SimpleFilter("`test`.`t1`.`pos` > 5000", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
            nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

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
                    new ParseTree(new SimpleFilter("`test`.`t1`.`place` > 'abcdefghij'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr),
                    new ParseTree(new SimpleFilter("`test`.`t1`.`posname` > 'qwer'",
                                                   SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                  nullptr, nullptr)),
                new ParseTree(new SimpleFilter("`test`.`t1`.`rid` > 20",
                                               SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                              nullptr, nullptr)),
            new ParseTree(new SimpleFilter("`test`.`t1`.`pos` > 5000",
                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                          nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`test`.`t1`.`place` < 'zyxqwertyu'",
                                       SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                      nullptr, nullptr)),
    new ParseTree(
        new SimpleFilter("`test`.`t1`.`id` < 30", SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}), nullptr,
        nullptr));
}

#include "simplecolumn.h"
#include "constantfilter.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include "parsetree.h"

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
                                        new ParseTree(new SimpleFilter(
                                                          "`tpch`.`PART`.`p_size` <= 15",
                                                          SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                                      nullptr, nullptr),
                                        new ParseTree(new SimpleFilter(
                                                          "`tpch`.`LINEITEM`.`l_quantity` <= 30",
                                                          SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                                      nullptr, nullptr)),
                                    new ParseTree(
                                        new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 20",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr)),
                                new ParseTree(
                                    new ConstantFilter(
                                        boost::shared_ptr<Operator>(new LogicOperator("or")),
                                        ConstantFilter::FilterList{
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'LG PKG'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'LG PACK'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'LG BOX'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'LG CASE'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}))},
                                        boost::shared_ptr<ReturnedColumn>(
                                            new SimpleColumn("`tpch`.`PART`.`p_container`",
                                                             SimpleColumn::ForTestPurposeWithoutOID{})),
                                        "",
                                        "`tpch`.`PART`.`P_CONTAINER` in ('LG CASE','LG BOX','LG PACK','LG "
                                        "PKG')"),
                                    nullptr, nullptr)),
                            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#34'",
                                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                          nullptr, nullptr)),
                        new ParseTree(
                            new LogicOperator("and"),
                            new ParseTree(
                                new LogicOperator("and"),
                                new ParseTree(
                                    new LogicOperator("and"),
                                    new ParseTree(
                                        new LogicOperator("and"),
                                        new ParseTree(new SimpleFilter(
                                                          "`tpch`.`PART`.`p_size` <= 10",
                                                          SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                                      nullptr, nullptr),
                                        new ParseTree(new SimpleFilter(
                                                          "`tpch`.`LINEITEM`.`l_quantity` <= 20",
                                                          SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                                      nullptr, nullptr)),
                                    new ParseTree(
                                        new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 10",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr)),
                                new ParseTree(
                                    new ConstantFilter(
                                        boost::shared_ptr<Operator>(new LogicOperator("or")),
                                        ConstantFilter::FilterList{
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'MED PACK'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'MED PKG'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'MED BOX'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                            boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                "`tpch`.`PART`.`p_container` = 'MED BAG'",
                                                SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}))},
                                        boost::shared_ptr<ReturnedColumn>(
                                            new SimpleColumn("`tpch`.`PART`.`p_container`",
                                                             SimpleColumn::ForTestPurposeWithoutOID{})),
                                        "",
                                        "`tpch`.`PART`.`P_CONTAINER` in ('MED BAG','MED BOX','MED PKG','MED "
                                        "PACK')"),
                                    nullptr, nullptr)),
                            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#23'",
                                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                          nullptr, nullptr))),
                    new ParseTree(
                        new LogicOperator("and"),
                        new ParseTree(
                            new LogicOperator("and"),
                            new ParseTree(
                                new LogicOperator("and"),
                                new ParseTree(
                                    new LogicOperator("and"),
                                    new ParseTree(
                                        new SimpleFilter("`tpch`.`PART`.`p_size` <= 5",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr),
                                    new ParseTree(
                                        new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` <= 11",
                                                         SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                        nullptr, nullptr)),
                                new ParseTree(
                                    new SimpleFilter("`tpch`.`LINEITEM`.`l_quantity` >= 1",
                                                     SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                    nullptr, nullptr)),
                            new ParseTree(
                                new ConstantFilter(
                                    boost::shared_ptr<Operator>(new LogicOperator("or")),
                                    ConstantFilter::FilterList{
                                        boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                            "`tpch`.`PART`.`p_container` = 'SM PKG'",
                                            SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                        boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                            "`tpch`.`PART`.`p_container` = 'SM PACK'",
                                            SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                        boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                            "`tpch`.`PART`.`p_container` = 'SM BOX'",
                                            SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                        boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                            "`tpch`.`PART`.`p_container` = 'SM CASE'",
                                            SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}))},
                                    boost::shared_ptr<ReturnedColumn>(
                                        new SimpleColumn("`tpch`.`PART`.`p_container`",
                                                         SimpleColumn::ForTestPurposeWithoutOID{})),
                                    "",
                                    "`tpch`.`PART`.`P_CONTAINER` in ('SM CASE','SM BOX','SM PACK','SM PKG')"),
                                nullptr, nullptr)),
                        new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_brand` = 'Brand#12'",
                                                       SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                                      nullptr, nullptr))),
                new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_size` >= 1",
                                               SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                              nullptr, nullptr)),
            new ParseTree(new SimpleFilter("`tpch`.`PART`.`p_partkey` = `tpch`.`LINEITEM`.`l_partkey`",
                                           SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                          nullptr, nullptr)),
        new ParseTree(new SimpleFilter("`tpch`.`LINEITEM`.`l_shipinstruct` = 'DELIVER IN PERSON'",
                                       SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}),
                      nullptr, nullptr)),
    new ParseTree(new ConstantFilter(
                      boost::shared_ptr<Operator>(new LogicOperator("or")),
                      ConstantFilter::FilterList{boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                     "`tpch`.`LINEITEM`.`l_shipmode` = 'AIR REG'",
                                                     SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})),
                                                 boost::shared_ptr<SimpleFilter>(new SimpleFilter(
                                                     "`tpch`.`LINEITEM`.`l_shipmode` = 'AIR'",
                                                     SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}))},
                      boost::shared_ptr<ReturnedColumn>(new SimpleColumn(
                          "`tpch`.`LINEITEM`.`l_shipmode`", SimpleColumn::ForTestPurposeWithoutOID{})),
                      "", "`tpch`.`LINEITEM`.`L_SHIPMODE` in ('AIR','AIR REG')"),
                  nullptr, nullptr));
}
