.. _source_file:

Source file
===========

Usually, each UDA(n)F function will have just one .cpp. Be sure to write your header file first. It's much easier to implent the various parts if you have a template to work from.

The easiest way to create these files is to copy them an example closest to the type of function you intend to create.

You need a data structure to hold your aggregate values. You can either use :ref:`simpledatamodel`, or :ref:`complexdatamodel`.

You may only need a few accunulators and counters. These can be represented as a fixed size data structure. For these needs, you may choose :ref:`simpledatamodel`. Here's a struct for a possible AVG function::

	struct AVGdata
	{
		uint64_t	total;
		uint64_t	count;
	};

If you have a more complex data structure that may have varying size, you must use :ref:`complexdatamodel`. This should be defined in the header. Here's a struct for MEDIAN example from median.h:

.. literalinclude:: ../../../median.h
   :lines: 80-97
   :language: cpp

In each of the functions that have a context parameter, you should type cast the data member of context's UserData member::

	struct AVGdata* data = (struct allnull_data*)context->getUserData()->data;

Or, if using the :ref:`complexdatamodel`, type cast the UserData to your UserData derived struct::

	MedianData* data = static_cast<MedianData*>(context->getUserData());

init()
------

.. c:function:: ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

:param context: The context object for this call.

:param colTypes: A list of the ColumnDatum used to access column types of the parameters. In init(), the columnData member is invalid.

 see :ref:`ColumnDatum`. In Columnstore 1.2, An arbitrary number of parameters is supported.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

The init() method is where you sanity check the input datatypes, set the output type and set any run flags for this instance. init() is called one time from the mysqld process. All settings you do here are propagated through the system.

init() is the exception to type casting the UserData member of context. UserData has not been created when init() is called, so you shouldn't use it here. 

.. rubric:: Set User Data Size

If you're using :ref:`simpledatamodel`, you need to set the size of the structure::

 context->setUserDataSize(sizeof(allnull_data));

.. rubric:: Check parameter count and type

Each function expects a certain number of columns to be entered as parameters in the SQL query. It is possible to create a UDAF that accepts a variable number of parameters. You can discover which ones were actually used in init(), and modify your function's behavior accordingly.

colTypes is an array of ColumnData from which can be gleaned the type and name. The name is the column name from the SQL query. You can use this information to sanity check for compatible type(s) and also to modify your functions behavior based on type. To do this, add members to your data struct to be tested in the other Methods. Set these members based on colDataTypes (:ref:`ColDataTypes <coldatatype>`).

The actual number of paramters passed can be gotten from context->getParameterCount().
::

	if (context->getParameterCount() < 1)
	{
		// The error message will be prepended with
		// "The storage engine for the table doesn't support "
		context->setErrorMessage("allnull() with 0 arguments");
		return mcsv1_UDAF::ERROR;
	}

.. rubric:: Set the ResultType

When you create your function using the SQL CREATE FUNCTION command, you must include a result type in the command. However, you're not completely limited by that decision. You may choose to return a different type based on any number of factors, including the colTypes. setResultType accepts any of the CalpontSystemCatalog::ColType enum values(:ref:`ColDataTypes <coldatatype>`).

::

	context->setResultType(CalpontSystemCatalog::TINYINT);

.. rubric:: Set width and scale

If you have special requirements, especially if you might be dealing with decimal types::

	context->setColWidth(8);
	context->setScale(context->getScale()*2);
	context->setPrecision(19);


.. rubric:: Set runflags

There are a number of run flags that you can set. Most are for use as an analytic function (Window Function), but a useful one for all functions is UDAF_IGNORE_NULLS. see :ref:`Run Flags <runflags>` for a complete list::

	context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);

reset()
-------

.. c:function:: ReturnCode reset(mcsv1Context* context);

:param context: The context object for this call.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

The reset() method initializes the context for a new aggregation or sub-aggregation. 

Then initialize the data in whatever way you need to::

	data->mData.clear();

This function may be called multiple times from both the UM and the PM. Make no assumptions about useful data in UserData from call to call.

nextValue()
-----------

.. c:function:: ReturnCode nextValue(mcsv1Context* context, 				 ColumnDatum* valsIn);

:param context: The context object for this call

:param valsIn: an array representing the values to be added for each parameter for this row.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

nextValue() is called from the PM for aggregate usage and the UM for Analytic usage.

valsIn contains a vector of all the parameters from the function call in the SQL query.

Depending on your function, you may wish to be able to handle many different types of input. There's a helper template function convertAnyTo() which will convert the input static:any value to the designated type. For Example, if your internal accumulater is of type double, you might use::

	static_any::any& valIn = valsDropped[0].columnData;
	AVGData& data = static_cast<MedianData*>(context->getUserData())->mData;
	int64_t val = 0;

	if (valIn.empty())
	{
		return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
	}

	val = convertAnyTo<double>(valIn);

Once you've gotten your data in a format you like, then do your aggregation. For AVG, you might see::

   data.total = val;
   ++data.count;

subEvaluate
-----------

.. c:function:: ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn);

:param context: The context object for this call

:param userDataIn: A UserData struct representing the sub-aggregation

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

subEvaluate() is called on the UM for the consolidation of the subaggregations from the PM. The sub-aggregate from the PM is in userDataIn and the result is to be placed into the UserData struct of context. In this case, you need to type cast userDataIn in a similar fashion as you do the context's UserData struct.

For AVG, you might see::

	struct AVGdata* outData = (struct AVGdata*)context->getUserData()->data;
	struct AVGdata* inData = (struct AVGdata*)userDataIn->data;
	outData->total += inData->total;
	outData->count += inData->count;
	return mcsv1_UDAF::SUCCESS;

evaluate
--------

.. c:function:: ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);

:param context: The context object for this call

:param valOut [out]: The final value for this GROUP or WINDOW.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

evaluate() is where you do your final calculations. It's pretty straight forward and is seldom different for UDAF (aggregation) or UDAnF (analytic).

For AVG, you might see::

	int64_t avg;
	struct AVGdata* data = (struct AVGdata*)context->getUserData()->data;
	avg = data->total / data.count;
	valOut = avg;
	return mcsv1_UDAF::SUCCESS;

dropValue
---------

.. c:function:: ReturnCode dropValue(mcsv1Context* context, 				 ColumnDatum* valsDropped);

:param context: The context object for this call

:param valsDropped: a vector representing the values to be dropped for each parameter for this row.

dropValue is an optional method for optimizing UDAnF (Analytic Functions). When used as an aggregate UDAF, dropValue isn't called.

As a Window Moves, some values come into scope and some values leave scope. When values leave scope, dropValue is called so that we don't have to recalculate the whole Window. We just need to undo what was done in nextValue for the dropped entries.

Like nextValue, your function may be able to handle a whole range of data types: For AVG, you might have::

	static_any::any& valIn = valsDropped[0].columnData;
	AVGData& data = static_cast<MedianData*>(context->getUserData())->mData;
	int64_t val = 0;

	if (valIn.empty())
	{
		return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
	}

	if (valIn.compatible(charTypeId))
	{
		val = valIn.cast<char>();
	}
	else if (valIn.compatible(scharTypeId))
	{
		val = valIn.cast<signed char>();
	}
	else if (valIn.compatible(shortTypeId))
	{
		val = valIn.cast<short>();
	}
	.
	.
	.

	data.total -= val;
	--data.count;

	return mcsv1_UDAF::SUCCESS;

.. c:function:: ReturnCode createUserData(UserData*& userdata, int32_t& length);

:param userData [out]: A pointer to be allocated by the function.

:param length [out]: The length of the data allocated.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 See the chapter on :ref:`complexdatamodel` for more information on how to use this Method.


