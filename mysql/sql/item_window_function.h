/*
 
Copyright (C) 2009-2013 Calpont Corporation.

Use of and access to the Calpont InfiniDB Community software is subject to the
terms and conditions of the Calpont Open Source License Agreement. Use of and
access to the Calpont InfiniDB Enterprise software is subject to the terms and
conditions of the Calpont End User License Agreement.

This program is distributed in the hope that it will be useful, and unless
otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
Please refer to the Calpont Open Source License Agreement and the Calpont End
User License Agreement for more details.

You should have received a copy of either the Calpont Open Source License
Agreement or the Calpont End User License Agreement along with this program; if
not, it is your responsibility to review the terms and conditions of the proper
Calpont license agreement by visiting http://www.calpont.com for the Calpont
InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
the Calpont InfiniDB Community Calpont Open Source License Agreement.

Calpont may make changes to these license agreements from time to time. When
these changes are made, Calpont will make a new copy of the Calpont End User
License Agreement available at http://www.calpont.com and a new copy of the
Calpont Open Source License Agreement available at http:///www.infinidb.org.
You understand and agree that if you use the Program after the date on which
the license agreement authorizing your use has changed, Calpont will treat your
use as acceptance of the updated License.

*/

/***********************************************************************
*   $Id: item_window_function.h 9559 2013-05-22 17:58:13Z xlou $
*
*
***********************************************************************/

#ifndef ITEM_WINDOW_FUNCTION_H
#define ITEM_WINDOW_FUNCTION_H

#include "mysql_priv.h"

enum BOUND
{
	PRECEDING = 0,
	FOLLOWING,
	UNBOUNDED_PRECEDING,
	UNBOUNDED_FOLLOWING,
	CURRENT_ROW
};

struct Boundary
{
	Boundary();
	Boundary(BOUND b);
	~Boundary() {}
	bool fix_fields(THD* thd, Item** ref);
	Item* item; // has to evaluate to unsigned value
	enum BOUND bound;
};

struct Frame
{
	Frame();
	~Frame();
	bool fix_fields(THD* thd, Item** ref);
	Boundary* start;
	Boundary* end;
	bool isRange; // RANGE or ROWS
};

struct Ordering
{
	Ordering();
	~Ordering();
	bool fix_fields(THD* thd, Item** ref);
	SQL_LIST* orders;
	Frame* frame;
};

struct Window_context
{
	Window_context();
	~Window_context();
	Item** partitions;
	uint partition_count;
	Ordering* ordering;
	void setPartitions(List<Item>* list);
	void setOrders(Ordering* order) { ordering = order; }
	bool fix_fields(THD* thd, Item** ref);

};

class Item_func_window :public Item_func
{
public:
	Item_func_window(LEX_STRING name, 
	                 Window_context* ctx = NULL, 
	                 bool dist = false);
	Item_func_window(LEX_STRING name, 
	                 Item *arg, 
	                 Window_context* ctx = NULL, 
	                 bool dist = false);
	Item_func_window(LEX_STRING name, 
	                 Item *arg1, 
	                 Item *arg2, 
	                 Window_context* ctx = NULL, 
	                 bool dist = false);
	Item_func_window(LEX_STRING name, 
	                 Item *arg1, 
	                 Item *arg2, 
	                 Item* arg3, 
	                 Window_context* ctx = NULL, 
	                 bool dist = false);
	Item_func_window(LEX_STRING name, 
	                 Item *arg1, 
	                 Item *arg2, 
	                 Item* arg3, 
	                 Item* arg4,
	                 Window_context* ctx = NULL, 
	                 bool dist = false);
	virtual ~Item_func_window();
	const char *func_name() const { return funcname; }
	virtual void fix_length_and_dec();
	virtual enum Type type() const { return WINDOW_FUNC_ITEM; }

	// the following should probably error out because we'll never need mysql to evaluate
	// window functions.
	virtual double val_real();
	virtual longlong val_int();
	virtual String* val_str(String*);
	bool check_partition_func_processor(uchar *int_arg) {return FALSE;}
	void partition_by (List<Item>* list) { window_context->setPartitions(list); }
	Item** partition_by() {return window_context->partitions;}
	void order_by(Ordering* order) { window_context->setOrders(order); }
	Ordering* order_by() {return window_context->ordering;}
	virtual bool fix_fields(THD* thd, Item** ref);
	Window_context* window_ctx() { return window_context; }
	void window_ctx(Window_context* window_ctx) { window_context = window_ctx; }
	virtual enum Item_result result_type () const;
	void isDistinct(bool dist) { distinct = dist; }
	bool isDistinct() const { return distinct; }
  bool respectNulls; // for some window functions

protected:
	Window_context* window_context;
	Item_result hybrid_type;
	bool distinct;
	char* funcname;
};

// avg
class Item_func_window_avg : public Item_func_window
{
public:
	Item_func_window_avg(LEX_STRING name, 
	                     Item *arg, 
	                     Window_context* ctx = NULL, 
	                     bool dist = false) : 
	                     Item_func_window(name, arg, ctx, dist){}
	void fix_length_and_dec();
	enum Item_result result_type () const { return hybrid_type; }
	uint prec_increment;
};

// sum
class Item_func_window_sum : public Item_func_window
{
public:
	Item_func_window_sum(LEX_STRING name, 
	                     Item *arg, 
	                     Window_context* ctx = NULL, 
	                     bool dist = false) : 
	                     Item_func_window(name, arg, ctx, dist){}
	void fix_length_and_dec();
	enum Item_result result_type () const { return hybrid_type; }
};

// rank
class Item_func_window_rank : public Item_func_window
{
public:
	Item_func_window_rank(LEX_STRING name, 
		                  Window_context* ctx = NULL,
	                      bool dist = false) :
	                      Item_func_window(name, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
	virtual enum Item_result result_type () const;
	virtual void fix_length_and_dec();
	uint prec_increment;
};

// nth_value
class Item_func_window_nth_value : public Item_func_window
{
public:
	Item_func_window_nth_value(LEX_STRING name, 
	                           Item *arg1,
	                           Item *arg2,
	                           Item *arg3,
	                           Item *arg4,
	                           Window_context* ctx = NULL,
	                           bool dist = false) :
	                           Item_func_window(name, arg1, arg2, arg3, arg4, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
};

// statistic function: stddev_pop/stddev_samp, var_pop/var_samp
class Item_func_window_stats : public Item_func_window
{
public:
	Item_func_window_stats(LEX_STRING name, 
	                       Item *arg, 
	                       Window_context* ctx = NULL, 
	                       bool dist = false) : 
	                       Item_func_window(name, arg, ctx, dist){}
	virtual enum Item_result result_type () const;
	virtual void fix_length_and_dec();
	uint prec_increment;
};

// count
class Item_func_window_int : public Item_func_window
{
public:
	Item_func_window_int(LEX_STRING name, 
		                 Window_context* ctx = NULL,
	                     bool dist = false) :
	                     Item_func_window(name, ctx, dist){}
	Item_func_window_int(LEX_STRING name, 
	                     Item *arg, 
	                     Window_context* ctx = NULL, 
	                     bool dist = false) : 
	                     Item_func_window(name, arg, ctx, dist){}
	virtual enum Item_result result_type () const;
};

// lead/lag
class Item_func_window_lead_lag : public Item_func_window
{
public:
	Item_func_window_lead_lag(LEX_STRING name, 
	                          Item *arg1,
	                          Item *arg2,
	                          Item *arg3,
	                          Item *arg4,
	                          Window_context* ctx = NULL, 
	                          bool dist = false) :
	                          Item_func_window(name, arg1, arg2, arg3, arg4, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
};

// median
class Item_func_window_median : public Item_func_window
{
public:
	Item_func_window_median (LEX_STRING name, 
	                         Item *arg1,
	                         Window_context* ctx = NULL, 
	                         bool dist = false) :
	                         Item_func_window(name, arg1, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
	virtual void fix_length_and_dec();
	virtual enum Item_result result_type () const;
	uint prec_increment;
};

// row_number
class Item_func_window_rownumber : public Item_func_window
{
public:
	Item_func_window_rownumber (LEX_STRING name, 
	                            Window_context* ctx = NULL, 
	                            bool dist = false) :
	                            Item_func_window(name, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
};

// ntile
class Item_func_window_ntile : public Item_func_window
{
public:
	Item_func_window_ntile (LEX_STRING name, 
	                        Item *arg1,
	                        Window_context* ctx = NULL, 
	                        bool dist = false) :
	                        Item_func_window(name, arg1, ctx, dist){}
	virtual bool fix_fields(THD* thd, Item** ref);
};

// percentile
class Item_func_window_percentile : public Item_func_window
{
public:
	Item_func_window_percentile (LEX_STRING name, 
	                             Item *arg1,
	                             SQL_LIST* order,
	                             Window_context* ctx = NULL) :
	                               Item_func_window(name, arg1, ctx, false),
	                               group_order (order){}
	virtual bool fix_fields(THD* thd, Item** ref);
	virtual void fix_length_and_dec();
	SQL_LIST* group_order;
	uint prec_increment;
	virtual enum Item_result result_type () const;
};

#endif
