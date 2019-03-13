.. _mcsv1_udaf:

mcsv1_UDAF
==========

class mcsv1_UDAF is the class from which you derive your UDA(n)F class. There are a number of methodss which you must implement, and a couple that you only need for certain purposes.

All the methods except the constuctor and destructor take as parameters at least a pointer to the context object and return a status code. The context contains many useful tidbits of information, and most importantly, a copy of your userdata that you manipulate.

The base class has no data members. It is designed to be only a container for your callback methods. In most cases, UDA(n)F implementations should also not have any data members. There is no guarantee that the instance called at any given time is the exact instance called at another time. Data will not persist from call to call. This object is not streamed, and there is no expectation by the framework that there is data in it. 

However, adding static const members makes sense.

For UDAF (not Window Functions) Aggregation takes place in three stages:

* Subaggregation on the PM. nextValue()
* Consolodation on the UM. subevaluate()
* Evaluation of the function on the UM. evaluate()

There are situations where the system makes a choice to perform all UDAF calculations on the UM. The presence of group_concat() in the query and certain joins can cause the optimizer to make this choice.

For Window Functions, all aggregation occurs on the UM, and thus the subevaluate step is skipped. There is an optional dropValue() function that may be added.

* Aggregation on the UM. nextValue()
* Optional aggregation on the UM using dropValue()
* Evaluation of the function on the UM. evaluate()

There are a few ways that the system may call these methods for UDAnF.

.. rubric:: The default way. 

For each Window movement:

* call reset() to initialize a userData struct.
* call nextValue() for each value in the Window.
* call evaluate()

.. rubric:: If the Frame is UNBOUNDED PRECEDING to CURRENT ROW:

call reset() to initialize a userData struct.

For each Window movement:

* call nextValue() for each value entering the Window.
* call evaluate()

.. rubric:: If dropValue() is defined:

call reset() to initialize a userData struct.

For each Window movement:

* call dropValue() for each value leaving the Window.
* call nextValue() for each value entering the Window.
* call evaluate()

return code
-----------

Each function returns a ReturnCode (enum) it may be one of the following:

* ERROR = 0,
* SUCCESS = 1,
* NOT_IMPLEMENTED = 2   // User UDA(n)F shouldn't return this

Constructor
-----------

.. c:function:: mcsv1_UDAF();

 There are no base data members, so the constructor does nothing.

Destructor
----------

.. c:function:: virtual ~mcsv1_UDAF();

 The base destructor does nothing.

Callback Methods
----------------

.. _init:

.. c:function:: ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes);

:param context: The context object for this call.

:param colTypes: A list of ColumnDatum structures. Use this to access the column types of the parameters. colTypes.columnData will be invalid.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS
 
 Use init() to initialize flags and instance data. Init() will be called only once for any SQL query.

 This is where you should check for data type compatibility and set the return type if desired.

 If you see any issue with the data types or any other reason the function may fail, return ERROR, otherwise, return SUCCESS.

 All flags, return type, Window Frame, etc. set in the context object here will be propogated to any copies made and will be streamed to the various modules. You are guaranteed that these settings will be correct for each callback.

.. _reset:

.. c:function:: ReturnCode reset(mcsv1Context* context);

:param context: The context object for this call.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 Reset the UDA(n)F for a new group, partition or, in some cases, new Window Frame. Do not free any memory directly allocated by createUserData(). The SDK Framework owns that memory and will handle that. However, Empty any user defined containers and free memory you allocated in other callback methods. Use this opportunity to reset any variables in your user data needed for the next aggregation. 

 Use context->getUserData() and type cast it to your UserData type or Simple Data Model stuct. 

.. _nextvalue:

.. c:function:: ReturnCode nextValue(mcsv1Context* context, 				 ColumnDatum* valsIn);

:param context: The context object for this call

:param valsIn: an array representing the values to be added for each parameter for this row.
 
:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 Use context->getUserData() and type cast it to your UserData type or Simple Data Model stuct. 

 nextValue() is called for each Window movement that passes the WHERE and HAVING clauses. The context's UserData will contain values that have been sub-aggregated to this point for the group, partition or Window Frame. nextValue is called on the PM for aggregation and on the UM for Window Functions.

 When used in an aggregate, the function should not rely on order or completeness since the sub-aggregation is going on at the PM, it only has access to the data stored on the PM's dbroots.

 When used as a analytic function (Window Function), nextValue is called for each Window movement in the Window. If dropValue is defined, then it may be called for every value leaving the Window, and nextValue called for each new value entering the Window.

 Since this may called for every row, it is important that this method be efficient.

.. _subevaluate:

.. c:function:: ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn);

:param context: The context object for this call

:param userDataIn: A UserData struct representing the sub-aggregation

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 subEvaluate() is the middle stage of aggregation and runs on the UM. It should take the sub-aggregations from the PM's as filled in by nextValue(), and finish the aggregation.

 The userData struct in context will be newly initialized for the first call to subEvaluate for each GROUP BY. userDataIn will have the final values as set by nextValue() for a given PM and GROUP BY. 

 Each call to subEvaluate should aggregate the values from userDataIn into the context's UserData struct.

.. _evaluate:

.. c:function:: ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut);

:param context: The context object for this call

:param valOut [out]: The final value for this GROUP or WINDOW.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 evaluate() is the final stage of aggregation for all User Define Aggregate or Analytic Functions -- UDA(n)F. 

 For aggregate (UDAF) use, the context's UserData struct will have the values as set by the last call to subEvaluate for a specific GROUP BY.

 For analytic use (UDAnF) the context's UserData struct will have the values as set by the latest call to nextValue() for the Window.

 Set your aggregated value into valOut. The type you set should be compatible with the type defined in the context's result type. The framework will do it's best to do any conversions if required.

.. _dropvalue:

.. c:function:: ReturnCode dropValue(mcsv1Context* context, 				 ColumnDatum* valsDropped);

:param context: The context object for this call

:param valsDropped: an array representing the values to be dropped for each parameter for this row.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 If dropValue() is defined, it will optimize most calls as an analytic function. If your UDAnF will always be called with a Window Frame of UNBOUNDED PRECEDING to CURRENT ROW, then dropValue will never be called. For other Frames, dropValue can speed things up. There are cases where dropValue makes no sense. If you can't undo what nextValue() does, then dropValue won't work.

 dropValue() should perform the reverse of the actions taken by nextValue() for each Window movement.

 For example, for an AVG function::

  nextValue:
   Add the value to accumulator
   increment row count

  dropValue:
   Subtract the value from accumulator
   decrement row count

.. _createuserdata:

.. c:function:: ReturnCode createUserData(UserData*& userdata, int32_t& length);

:param userData [out]: A pointer to be allocated by the function.

:param length [out]: The length of the data allocated.

:returns: ReturnCode::ERROR or ReturnCode::SUCCESS

 See the chapter on :ref:`complexdatamodel` for more information on how to use this Method.

 New a UserData derived structure and return a pointer to it. Set length to the base length of the structure.

