/*
   Copyright (c) 2019 MariaDB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


// ha_calpont.cpp includes this file.

/*@brief  check_walk - It traverses filter conditions*/
/************************************************************
 * DESCRIPTION:
 * It traverses filter predicates looking for unsupported
 * JOIN types: non-equi JOIN, e.g  t1.c1 > t2.c2;
 * logical OR.
 * PARAMETERS:
 *    thd - THD pointer.
 *    derived - TABLE_LIST* to work with.
 * RETURN:
 *    derived_handler if possible
 *    NULL in other case
 ***********************************************************/
void check_walk(const Item* item, void* arg)
{
    bool* unsupported_feature  = static_cast<bool*>(arg);
    if ( *unsupported_feature )
        return;
    switch (item->type())
    {
        case Item::FUNC_ITEM:
        {
            const Item_func* ifp = static_cast<const Item_func*>(item);

            if ( ifp->functype() != Item_func::EQ_FUNC ) // NON-equi JOIN
            {
                if ( ifp->argument_count() == 2 &&
                    ifp->arguments()[0]->type() == Item::FIELD_ITEM &&
                    ifp->arguments()[1]->type() == Item::FIELD_ITEM )
                {
                    Item_field* left= static_cast<Item_field*>(ifp->arguments()[0]);
                    Item_field* right= static_cast<Item_field*>(ifp->arguments()[1]);

                    if ( left->field->table != right->field->table )
                    {
                        *unsupported_feature = true;
                        return;
                    }
                }
                else // IN + correlated subquery
                {
                    if ( ifp->functype() == Item_func::NOT_FUNC
                        && ifp->arguments()[0]->type() == Item::EXPR_CACHE_ITEM )
                    {
                        check_walk(ifp->arguments()[0], arg);
                    }
                }
            }
            break;
        }

        case Item::EXPR_CACHE_ITEM: // IN + correlated subquery
        {
            const Item_cache_wrapper* icw = static_cast<const Item_cache_wrapper*>(item);
            if ( icw->get_orig_item()->type() == Item::FUNC_ITEM )
            {
                const Item_func *ifp = static_cast<const Item_func*>(icw->get_orig_item());
                if ( ifp->argument_count() == 2 &&
                    ( ifp->arguments()[0]->type() == Item::Item::SUBSELECT_ITEM
                    || ifp->arguments()[1]->type() == Item::Item::SUBSELECT_ITEM ))
                {
                    *unsupported_feature = true;
                    return;
                }
            }
            break;
        }

        case Item::COND_ITEM: // OR in JOIN conds is unsupported yet
        {
            Item_cond* icp = (Item_cond*)item;
            if ( is_cond_or(icp) )
            {
                *unsupported_feature = true;
            }
            break;
        }
        default:
        {
            break;
        }
    }
}

/*@brief  create_calpont_group_by_handler- Creates handler*/
/***********************************************************
 * DESCRIPTION:
 * Creates a group_by pushdown handler if there is no:
 *  non-equi JOIN, e.g * t1.c1 > t2.c2
 *  logical OR in the filter predicates
 *  Impossible WHERE
 *  Impossible HAVING
 * and there is either GROUP BY or aggregation function
 * exists at the top level.
 * Valid queries with the last two crashes the server if
 * processed.
 * Details are in server/sql/group_by_handler.h
 * PARAMETERS:
 *    thd - THD pointer
 *    query - Query structure LFM in group_by_handler.h
 * RETURN:
 *    group_by_handler if success
 *    NULL in other case
 ***********************************************************/
static group_by_handler*
create_calpont_group_by_handler(THD* thd, Query* query)
{
    ha_calpont_group_by_handler* handler = NULL;
    // same as thd->lex->current_select
    SELECT_LEX *select_lex = query->from->select_lex;

    // Create a handler if query is valid. See comments for details.
    if ( thd->infinidb_vtable.vtable_state == THD::INFINIDB_DISABLE_VTABLE
        && ( thd->variables.infinidb_vtable_mode == 0
            || thd->variables.infinidb_vtable_mode == 2 )
        && ( query->group_by || select_lex->with_sum_func ) )
    {
        bool unsupported_feature = false;
        // revisit SELECT_LEX for all units
        for(TABLE_LIST* tl = query->from; !unsupported_feature && tl; tl = tl->next_global)
        {
            select_lex = tl->select_lex;
            // Correlation subquery. Comming soon so fail on this yet.
            unsupported_feature = select_lex->is_correlated;

            // Impossible HAVING or WHERE
            if ( ( !unsupported_feature && query->having && select_lex->having_value == Item::COND_FALSE )
                || ( select_lex->cond_count > 0
                    && select_lex->cond_value == Item::COND_FALSE ) )
            {
                unsupported_feature = true;
            }

            // Unsupported JOIN conditions
            if ( !unsupported_feature )
            {
                JOIN *join = select_lex->join;
                Item_cond *icp = 0;

                if (join != 0)
                    icp = reinterpret_cast<Item_cond*>(join->conds);

                if ( unsupported_feature == false
                    && icp )
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }

                // Optimizer could move some join conditions into where
                if (select_lex->where != 0)
                    icp = reinterpret_cast<Item_cond*>(select_lex->where);

                if ( unsupported_feature == false
                    && icp )
                {
                    icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
                }

            }
        } // unsupported features check ends here

        if ( !unsupported_feature )
        {
            handler = new ha_calpont_group_by_handler(thd, query);

            // Notify the server, that CS handles GROUP BY, ORDER BY and HAVING clauses.
            query->group_by = NULL;
            query->order_by = NULL;
            query->having = NULL;
        }
    }

    return handler;
}

/*@brief  create_columnstore_derived_handler- Creates handler*/
/************************************************************
 * DESCRIPTION:
 * Creates a derived handler if there is no non-equi JOIN, e.g
 * t1.c1 > t2.c2 and logical OR in the filter predicates.
 * More details in server/sql/derived_handler.h
 * PARAMETERS:
 *    thd - THD pointer.
 *    derived - TABLE_LIST* to work with.
 * RETURN:
 *    derived_handler if possible
 *    NULL in other case
 ***********************************************************/
static derived_handler*
create_columnstore_derived_handler(THD* thd, TABLE_LIST *derived)
{
    ha_columnstore_derived_handler* handler = NULL;
    handlerton *ht= 0;

    SELECT_LEX_UNIT *unit= derived->derived;

    if ( thd->infinidb_vtable.vtable_state != THD::INFINIDB_DISABLE_VTABLE
            && thd->variables.infinidb_vtable_mode != 0 )
    {
        return 0;
    }

    for (SELECT_LEX *sl= unit->first_select(); sl; sl= sl->next_select())
    {
        if (!(sl->join))
            return 0;
        for (TABLE_LIST *tbl= sl->join->tables_list; tbl; tbl= tbl->next_local)
        {
            if (!tbl->table)
                return 0;
            // Same handlerton type check.
            if (!ht)
                ht= tbl->table->file->partition_ht();
            else if (ht != tbl->table->file->partition_ht())
                return 0;
        }
    }

    bool unsupported_feature = false;
    {
        SELECT_LEX select_lex = *unit->first_select();
        JOIN* join = select_lex.join;
        Item_cond* icp = 0;

        if (join != 0)
            icp = reinterpret_cast<Item_cond*>(join->conds);

        if (!join)
        {
            icp = reinterpret_cast<Item_cond*>(select_lex.where);
        }

        if ( icp )
        {
            icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
        }
    }

    if ( !unsupported_feature )
        handler= new ha_columnstore_derived_handler(thd, derived);

  return handler;
}

/***********************************************************
 * DESCRIPTION:
 * derived_handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    tbl - tables involved.
 ***********************************************************/
ha_columnstore_derived_handler::ha_columnstore_derived_handler(THD *thd,
                                                             TABLE_LIST *dt)
  : derived_handler(thd, calpont_hton)
{
  derived = dt;
}

/***********************************************************
 * DESCRIPTION:
 * derived_handler destructor
 ***********************************************************/
ha_columnstore_derived_handler::~ha_columnstore_derived_handler()
{}

/*@brief  Initiate the query for derived_handler           */
/***********************************************************
 * DESCRIPTION:
 * Execute the query and saves derived table query.
 * ATM this function sets vtable_state and restores it afterwards
 * since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 ***********************************************************/
int ha_columnstore_derived_handler::init_scan()
{
    char query_buff[4096];

    DBUG_ENTER("ha_columnstore_derived_handler::init_scan");

    // Save query for logging
    String derived_query(query_buff, sizeof(query_buff), thd->charset());
    derived_query.length(0);
    derived->derived->print(&derived_query, QT_ORDINARY);

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;

    mcs_handler_info mhi = mcs_handler_info(static_cast<void*>(this), DERIVED);
    // this::table is the place for the result set
    int rc = ha_cs_impl_pushdown_init(&mhi, table);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/*@brief  Fetch next row for derived_handler               */
/***********************************************************
 * DESCRIPTION:
 * Fetches next row and saves it in the temp table
 * ATM this function sets vtable_state and restores it
 * afterwards since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_derived_handler::next_row()
{
    DBUG_ENTER("ha_columnstore_derived_handler::next_row");

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;

    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;

    int rc = ha_calpont_impl_rnd_next(table->record[0], table);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/*@brief  Finishes the scan and clean it up               */
/***********************************************************
 * DESCRIPTION:
 * Finishes the scan for derived handler
 * ATM this function sets vtable_state and restores it
 * afterwards since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_derived_handler::end_scan()
{
    DBUG_ENTER("ha_columnstore_derived_handler::end_scan");

    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_SELECT_VTABLE;

    int rc = ha_calpont_impl_rnd_end(table, true);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

void ha_columnstore_derived_handler::print_error(int, unsigned long)
{
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    query - Query describing structure
 ***********************************************************/
ha_calpont_group_by_handler::ha_calpont_group_by_handler(THD* thd_arg, Query* query)
        : group_by_handler(thd_arg, calpont_hton),
          select(query->select),
          table_list(query->from),
          distinct(query->distinct),
          where(query->where),
          group_by(query->group_by),
          order_by(query->order_by),
          having(query->having)
{
}

/***********************************************************
 * DESCRIPTION:
 * GROUP BY destructor
 ***********************************************************/
ha_calpont_group_by_handler::~ha_calpont_group_by_handler()
{
}

/***********************************************************
 * DESCRIPTION:
 * Makes the plan and prepares the data
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::init_scan()
{
    DBUG_ENTER("ha_calpont_group_by_handler::init_scan");

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    // MCOL-1052 Should be removed after cleaning the code up.
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;
    int rc = ha_calpont_impl_group_by_init(this, table);
    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Fetches a row and saves it to a temporary table.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::next_row()
{
    DBUG_ENTER("ha_calpont_group_by_handler::next_row");
    int rc = ha_calpont_impl_group_by_next(this, table);

    DBUG_RETURN(rc);
}

/***********************************************************
 * DESCRIPTION:
 * Shuts the scan down.
 * RETURN:
 *    int rc
 ***********************************************************/
int ha_calpont_group_by_handler::end_scan()
{
    DBUG_ENTER("ha_calpont_group_by_handler::end_scan");

    int rc = ha_calpont_impl_group_by_end(this, table);

    DBUG_RETURN(rc);
}

/*@brief  create_columnstore_select_handler- Creates handler*/
/************************************************************
 * DESCRIPTION:
 * Creates a select handler if there is no non-equi JOIN, e.g
 * t1.c1 > t2.c2 and logical OR in the filter predicates.
 * More details in server/sql/select_handler.h
 * PARAMETERS:
 *    thd - THD pointer.
 *    sel - SELECT_LEX* that describes the query.
 * RETURN:
 *    select_handler if possible
 *    NULL in other case
 ***********************************************************/
static select_handler*
create_columnstore_select_handler(THD* thd, SELECT_LEX* select_lex)
{
    ha_columnstore_select_handler* handler = NULL;
    handlerton *ht= 0;

    // Return if vtable enabled.
    if ( thd->infinidb_vtable.vtable_state != THD::INFINIDB_DISABLE_VTABLE
            && thd->variables.infinidb_vtable_mode != 0 )
    {
        return 0;
    }
    for (SELECT_LEX* sl = select_lex;sl; sl= sl->next_select())
    {
        if (!(sl->join))
            return 0;
        for (TABLE_LIST *tbl= sl->join->tables_list; tbl; tbl= tbl->next_local)
        {
            if (!tbl->table)
                return 0;
            // Same handlerton type check.
            if (!ht)
                ht= tbl->table->file->partition_ht();
            else if (ht != tbl->table->file->partition_ht())
                return 0;
        }
    }

    bool unsupported_feature = false;
    // Impossible HAVING or WHERE
    if ( ( select_lex->having && select_lex->having_value == Item::COND_FALSE )
        || ( select_lex->cond_count > 0
            && select_lex->cond_value == Item::COND_FALSE ) )
    {
        unsupported_feature = true;
    }

    // Unsupported query check.
    if ( !unsupported_feature )
    {
        // JOIN expression from WHERE, ON expressions
        JOIN* join = select_lex->join;
        Item_cond* where_icp = 0;
        Item_cond* on_icp = 0;

        if (join != 0)
        {
            where_icp = reinterpret_cast<Item_cond*>(join->conds);
        }

        if ( where_icp )
        {
            where_icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
        }

        // Looking for JOIN with ON expression through
        // TABLE_LIST in FROM until CS meets unsupported feature
        TABLE_LIST* table_ptr = select_lex->get_table_list();
        for (; !unsupported_feature && table_ptr; table_ptr = table_ptr->next_global)
        {
            if(table_ptr->on_expr)
            {
                on_icp = reinterpret_cast<Item_cond*>(table_ptr->on_expr);
                on_icp->traverse_cond(check_walk, &unsupported_feature, Item::POSTFIX);
            }
        }

        // CROSS JOIN w/o conditions isn't supported until MCOL-301
        // is ready.
        if (join && join->table_count >= 2 && ( !where_icp && !on_icp ))
        {
            unsupported_feature = true;
        }
    }
  
    if (!unsupported_feature)
    {
        handler = new ha_columnstore_select_handler(thd, select_lex);
    }

  return handler;
}

/***********************************************************
 * DESCRIPTION:
 * select_handler constructor
 * PARAMETERS:
 *    thd - THD pointer.
 *    select_lex - sematic tree for the query.
 ***********************************************************/
ha_columnstore_select_handler::ha_columnstore_select_handler(THD *thd,
                                                             SELECT_LEX* select_lex)
  : select_handler(thd, calpont_hton)
{
  select = select_lex;
}

/***********************************************************
 * DESCRIPTION:
 * select_handler constructor
 ***********************************************************/
ha_columnstore_select_handler::~ha_columnstore_select_handler()
{}

/*@brief  Initiate the query for select_handler           */
/***********************************************************
 * DESCRIPTION:
 * Execute the query and saves select table query.
 * ATM this function sets vtable_state and restores it afterwards
 * since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 ***********************************************************/
int ha_columnstore_select_handler::init_scan()
{
    char query_buff[4096];

    DBUG_ENTER("ha_columnstore_select_handler::init_scan");

    // Save query for logging
    String select_query(query_buff, sizeof(query_buff), thd->charset());
    select_query.length(0);
    select->print(thd, &select_query, QT_ORDINARY);

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;

    mcs_handler_info mhi = mcs_handler_info(static_cast<void*>(this), SELECT);
    // this::table is the place for the result set
    int rc = ha_cs_impl_pushdown_init(&mhi, table);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/*@brief  Fetch next row for select_handler               */
/***********************************************************
 * DESCRIPTION:
 * Fetches next row and saves it in the temp table
 * ATM this function sets vtable_state and restores it
 * afterwards since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_select_handler::next_row()
{
    DBUG_ENTER("ha_columnstore_select_handler::next_row");

    // Save vtable_state to restore the after we inited.
    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;

    thd->infinidb_vtable.vtable_state = THD::INFINIDB_CREATE_VTABLE;

    int rc = ha_calpont_impl_rnd_next(table->record[0], table);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

/*@brief  Finishes the scan and clean it up               */
/***********************************************************
 * DESCRIPTION:
 * Finishes the scan for select handler
 * ATM this function sets vtable_state and restores it
 * afterwards since it reuses existed vtable code internally.
 * PARAMETERS:
 *
 * RETURN:
 *    rc as int
 *
 ***********************************************************/
int ha_columnstore_select_handler::end_scan()
{
    DBUG_ENTER("ha_columnstore_select_handler::end_scan");

    THD::infinidb_state oldState = thd->infinidb_vtable.vtable_state;
    thd->infinidb_vtable.vtable_state = THD::INFINIDB_SELECT_VTABLE;

    int rc = ha_calpont_impl_rnd_end(table, true);

    thd->infinidb_vtable.vtable_state = oldState;

    DBUG_RETURN(rc);
}

void ha_columnstore_select_handler::print_error(int, unsigned long)
{}
