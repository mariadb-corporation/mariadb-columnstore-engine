mcsv1Context -- the Context object
----------------------------------

The class mcsv1Context holds all the state data for a given instance of the UDAF. There are parts that are set by user code to select options, and parts that are set by the Framework to tell you what is happening for a specific method call.

The class is designed to be accessed via functions. There should be no situations that require direct manipulation of the data items. To do so could prove counter-productive.

There are a set of public functions tagged "For use by the framework". You should not use these functions. To do so could prove counter-productive.

An instance of mcsv1Context is created and sent to your init() method. Here is where you set options and the like. Thereafter, a context is sent to each method. It will contain the options you set as well as state data specific for that call. However, it will not be the original context, but rather a copy. init() is called by the mysqld process. Other methods are called by ExeMgr (UM) and PrimProc (PM). The context is streamed from process to process and even within processes, it may be copied for multi-thread processing.

The mcsv1Context member functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. rubric:: constructor

.. c:function:: mcsv1Context();

   Sets some defaults. No magic here.

   EXPORT mcsv1Context(const mcsv1Context& rhs);
  
   Copy contstructor. The engine uses this to copy the context object for various reasons. You should not need to ever construct or copy a context object.

.. rubric:: destructor

.. c:function:: virtual ~mcsv1Context();

   The destructor is virtual only in case a version 2 is made. This supports backward compatibility. mcsv1Context should never be subclassed by UDA(n)F developers.

.. rubric:: Error message handling

.. c:function:: void setErrorMessage(std::string errmsg);

:param errmsg: The error message you would like to display.
:returns: nothing

 If an error occurs during your processing, you can send an error message to the system that will get displayed on the console and logged. You should also return ERROR from the method that set the error message.

.. c:function:: const std::string& getErrorMessage() const;

:returns: The current error message, if set.

 In case you want to know the current error message. In most (all?) cases, this will be an empty string unless you set it earlier in the same method, but is included for completeness.

.. _runflags:

.. rubric:: Runtime Flags

The runtime flags are 8 byte bit flag field where you set any options you need for the run. Set these options in your init() method. Setting flags outside of the init() method may have unintended consequences.

The following table lists the possible option flags. Many of these flags affect the behavior of UDAnF (Analytic Window Functions). Most UDAF can also be used as Window Functions as well. The flags that affect UDAnF are ignored when called as an aggregate function.

.. list-table:: Option Flags
   :widths: 20 5 30
   :header-rows: 1

   * - Option Name
     - Default
     - Usage
   * - UDAF_OVER_REQUIRED
     - Off
     - Tells the system to reject any call as an aggregate function. This function is a Window Function only.
   * - UDAF_OVER_ALLOWED
     - On
     - Tells the systen this function may be called as an aggregate or as a Window Function. UDAF_OVER_REQUIRED takes precedence. If not set, then the function may only be called as an aggregate function.
   * - UDAF_ORDER_REQUIRED
     - Off
     - Tells the system that if the function is used as a Window Function, then it must have an ORDER BY clause in the Window description.
   * - UDAF_ORDER_ALLOWED
     - On
     - Tells the system that if the function is used as a Window Function, then it may optionally have an ORDER BY clause in the Window description. If this flag is off, then the function may not have an ORDER BY clause.
   * - UDAF_WINDOWFRAME_REQUIRED
     - Off
     - Tells the system that if the function is used as a Window Function, then it must have a Window Frame defined. The Window Frame clause is the PRECEDING and FOLLOWING stuff.
   * - UDAF_WINDOWFRAME_ALLOWED
     - On
     - Tells the system that if the function is used as a Window Function, then it may optionally have a Window Frame defined. If this flag is off, then the function may not have a Window Frame clause.
   * - UDAF_MAYBE_NULL
     - On
     - Tells the system that the function may return a null value. Currently, this flag is ignored.
   * - UDAF_IGNORE_NULLS
     - On
     - Tells the system not to send NULL values to the function. They will be ignored. If off, then NULL values will be sent to the function.

.. c:function:: uint64_t setRunFlags(uint64_t flags);

:param flags: The new set of run flags to be used for this instance.
:returns: The previous set of flags.

 Replace the current set of flags with the new set. In general, this shouldn't be done unless you are absolutely sure. Rather, use setRunFlag() to set set specific options and clearRunFlag to turn of specific options.

.. c:function:: uint64_t getRunFlags() const;

:returns: The current set of run flags.

 Get the integer value of the run flags. You can use the result with the '|' operator to determine the setting of a specific option.

.. c:function::  bool setRunFlag(uint64_t flag);

:param flag: The flag or flags ('|' together) to be set.
:returns: The previous setting of the flag or flags (using &)

   Set a specific run flag or flags.

   Ex: setRunFlag(UDAF_OVER_REQUIRED | UDAF_ORDER_REQUIRED);

.. c:function::  bool getRunFlag(uint64_t flag);

:param flag: A flag or flags ('|' together) to get the value of.
:returns: The value of the flag or flags ('&' together).

 Get a specific flags value. If used with multiple flag values joined with the '|' operator, then all flags listed must be set for a true return value.

.. c:function::  bool clearRunFlag(uint64_t flag);

:param flag: A flag or flags ('|' together) to be cleared.
:returns: The previous value of the flag or flags ('&' together).

 Clear a specific flag and return it's previous value. If multiple flags are listed joined with the '|' operator, then all listed flags are cleared and will return true only if all flags had been set.

.. c:function:: bool toggleRunFlag(uint64_t flag);

:param flag: A flag to be toggled.
:returns: The previous value of the flag.

   Toggle a flag and return its previous setting.

.. rubric:: Runtime Environment

Use these to determine the way your UDA(n)F was called

.. c:function:: bool isAnalytic();

:returns: true if the current call of the function is as a Window Function.

.. c:function:: bool isWindowHasCurrentRow();

:returns: true if the Current Row is in the Window. This is usually true but may not be for some Window Frames such as, for exampe,  BETWEEEN UNBOUNDED PRECEDING AND 2 PRECEDDING. 

.. c:function:: bool isUM();

:returns: true if the call is from the UM.

.. c:function:: bool isPM();

:returns: true if the call is from the PM.

.. rubric:: Parameter refinement description accessors

.. c:function:: size_t getParameterCount() const;

:returns: the number of parameters to the function in the SQL query. 

.. c:function:: bool isParamNull(int paramIdx);

:returns: true if the parameter is NULL for the current row.

.. c:function:: bool isParamConstant(int paramIdx);

:returns: true if the parameter is a constant.

.. rubric:: Result Type

.. c:function:: CalpontSystemCatalog::ColDataType getResultType() const;

:returns: The result type. This will be set based on the CREATE FUNCTION SQL statement until over-ridden by setResultType().

.. c:function:: int32_t getScale() const;

:returns The currently set scale.

 Scale is the number of digits to the right of the decimal point. This value is ignored if the type isn't decimal and is usually set to 0 for non-decimal types.

.. c:function:: int32_t getPrecision() const;

:returns: The currently set precision

 Precision is the total number of decimal digits both on the left and right of the decimal point. This value is ignored for non-decimal types and may be 0, -1 or based on the max digits of the integer type.

.. c:function:: bool setResultType(CalpontSystemCatalog::ColDataType resultType);

:param: The new result type for the UDA(n)F
:returns: true always

 If you wish to set the return type based on the input type, then use this function in your init() method.

.. c:function:: bool setScale(int32_t scale);

:param: The new scale for the return type of the UDA(n)F
:returns: true always

 Scale is the number of digits to the right of the decimal point. This value is ignored if the type isn't decimal and is usually set to 0 for non-decimal types.

.. c:function:: bool setPrecision(int32_t precision);

:param: The new precision for the return type of the UDA(n)F
:returns: true always

 Precision is the total number of decimal digits both on the left and right of the decimal point. This value is ignored for non-decimal types and may be 0, -1 or based on the max digits of the integer type.

.. c:function:: int32_t getColWidth();

:returns: The current column width of the return type.

 For all types, get the return column width in bytes. Ex. INT will return 4. VARCHAR(255) will return 255 regardless of any contents.

.. c:function:: bool setColWidth(int32_t colWidth);

:param: The new column width for the return type of the UDA(n)F
:returns: true always

 For non-numric return types, set the return column width. This defaults to the the max length of the input data type.

.. rubric:: system interruption

.. c:function:: bool getInterrupted() const;

:returns: true if any thread for this function set an error.

 If a method is known to take a while, call this periodically to see if something interupted the processing. If getInterrupted() returns true, then the executing method should clean up and exit.

.. rubric:: User Data

.. c:function:: void  setUserDataSize(int bytes);

:param bytes: The number of bytes to be reserved for working memory.

 Sets the size of instance specific memory for :ref:`simpledatamodel`. This value is ignored if using :ref:`complexdatamodel` unless you specifically use it.

.. c:function:: UserData* getUserData();

:returns: A pointer to the current set of user data. 

 Type cast to your user data structure if using :ref:`complexdatamodel`. This is the function to call in each of your methods to get the current working copy of your user data.

.. rubric:: Window Frame

All UDAnF need a default Window Frame. If none is set here, the default is UNBOUNDED PRECEDING to CURRENT ROW. 

It's possible to not allow the the WINDOW FRAME phrase in the UDAnF by setting the UDAF_WINDOWFRAME_REQUIRED and UDAF_WINDOWFRAME_ALLOWED both to false. However, Columnstore requires a Window Frame in order to process UDAnF. In this case, the default will be used for all calls.

Possible values for start frame are:

* WF_UNBOUNDED_PRECEDING 
* WF_CURRENT_ROW
* WF_PRECEDING
* WF_FOLLOWING

Possible values for end frame are:

* WF_CURRENT_ROW
* WF_UNBOUNDED_FOLLOWING
* WF_PRECEDING
* WF_FOLLOWING
	
If WF_PRECEEDING and/or WF_FOLLOWING, a start or end constant should be included to say how many preceeding or following is the default (the frame offset).

Window Frames are not allowed to have reverse values. That is, the start frame must preceed the end frame. You can't set start = WF_FOLLOWING and end = WF_PRECEDDING. Results are undefined.

.. c:function:: bool  setDefaultWindowFrame(WF_FRAME defaultStartFrame, WF_FRAME defaultEndFrame, int32_t startConstant = 0, int32_t endConstant = 0);

:param defaultStartFrame: An enum value from the list above.
:param defaultEndFrame: An enum value from the list above.
:param startConstant: An integer value representing the frame offset. This may be negative. Only used if start frame is one of WF_PRECEEDING or WF_FOLLOWING.
:param endConstant: An integer value representing the frame offset. This may be negative. Only used if start frame is one of WF_PRECEEDING or WF_FOLLOWING.

.. c:function:: void  getStartFrame(WF_FRAME& startFrame, int32_t& startConstant) const;

:param startFrame (out): Returns the start frame as set by the function call in the query, or the default if the query doesn't include a WINDOW FRAME clause.

:param startConstant (out): Returns the start frame offset. Only valid if startFrame is one of WF_PRECEEDING or WF_FOLLOWING.

.. c:function:: void  getEndFrame(WF_FRAME& endFrame, int32_t& endConstant) const;

:param endFrame (out): Returns the end frame as set by the function call in the query, or the default if the query doesn't include a WINDOW FRAME clause.

:param endConstant (out): Returns the end frame offset. Only valid if endFrame is one of WF_PRECEEDING or WF_FOLLOWING.

.. rubric:: Deep Equivalence
.. c:function:: bool operator==(const mcsv1Context& c) const;
.. c:function:: bool operator!=(const mcsv1Context& c) const;

.. rubric:: string operator
.. c:function:: const std::string toString() const;

:returns: A string containing many of the values of the context in a human readable format for debugging purposes.

.. rubric:: The name of the function
.. c:function:: void setName(std::string name);

:param name: The name of the function.

 Setting the name of the function is optional. You can set the name in your init() method to be retrieved by other methods later. You may want to do this, for instance, if you want to use the UDA(n)F name in an error or log message.

.. c:function:: const std::string& getName() const;

:returns: The name of the function as set by setName().

