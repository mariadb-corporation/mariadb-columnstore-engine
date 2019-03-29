.. _memory_allocation:

Memory allocation and usage
===========================

Memory for the function is allocated and owned by the engine. In a distributed system like columnstore, memory must be allocated on each module. While it would be possible to let the API control memory, it's far simpler and safer to let the engine handle it.

There are two memory models available -- simple and complex.

Memory is allocated via callbacks to the mcsv1_UDAF::createUserData() method whose default implementation implements the Simple Data Model. If using the simple model, all you need to do is tell the system how much memory you need allocated. Everything else will be taken care of.

.. _simpledatamodel:

The Simple Data Model
---------------------

Many UDAF can be accomplished with a fixed memory model. For instance, an AVG function just needs an accumulator and a counter. This can be handled by two data items.

For the Simple Data Model, define a structure that has all the elements you need to do the function. Then, in your init() method, set the memory size you need. Each callback will have a pointer to a context whose data member is a pointer to the memory you need. Just cast to the structure you defined. allnull defines this structure:

.. literalinclude:: ../../../allnull.cpp
   :lines: 22-26
   :language: cpp

In the init method, it tells the framework how much memory it needs using the context's setUserDataSize() method:

.. literalinclude:: ../../../allnull.cpp
   :lines: 32
   :language: cpp


and uses it like this in the reset method:

.. literalinclude:: ../../../allnull.cpp
   :lines: 50-56
   :language: cpp

Notice that the memory is already allocated. All you do is typecast it.

.. _complexdatamodel:

The Complex Data Model
----------------------

There are functions where a fixed size memory chunk isn't sufficient, We need variable size data, containers of things, etc. The columnstore UDAF API supports these needs using the Complex Data Model.

This consists of the createUserData() method that must be over ridden and the UserData struct that must be extended to create your data class.

.. rubric:: struct UserData

Let's take a look at the base user data structure defined in mcsv1_udaf.h:

.. literalinclude:: ../../../mcsv1_udaf.h
   :lines: 131-173
   :language: cpp

There are two constructors listed. The second one taking a size is used by :ref:`simpledatamodel`.

Next you'll notice the serialize and unserialize methods. These must be defined for your function. The default just bit copies the contents of the data member, which is where memory for :ref:`simpledatamodel` is stored. 

To create a Complex Data Model, derive a class from UserData and create instances of the new class in your function's createUserData() method.

Memory is allocated on each module as we've already discussed. In a Complex Data Model, the engine has no idea how much memory needs to be passed from PM to UM after the intermediate steps of the aggregation have been accomplished. It handles this by calling streaming functions on the user data structure. This is what the serialize and unserialize methods are for.

It is very important that these two methods reflect each other exactly. If they don't, you will have alignment issues that most likely will lead to a SEGV signal 11.

In the MEDIAN UDAF, it works like this:

.. literalinclude:: ../../../median.h
   :lines: 80-97
   :language: cpp

Notice that the data is to be stored in a std::map container.

The serialize method leverages Columnstore's :ref:`ByteStream <bytestream>` class (in namespace messageqcpp). This is not optional. The serialize and unserialize methods are each passed an reference to a :ref:`ByteStream <bytestream>`. The framework expects the data to be streamed into the :ref:`ByteStream <bytestream>` by serialize and streamed back into your data struct by unserialize. See the chapter on :ref:`ByteStream <bytestream>` for more information.

For MEDIAN, serialize() iterates over the set and streams the values to the :ref:`ByteStream <bytestream>` and unserialze unstreams them back into the set:

.. literalinclude:: ../../../median.cpp
   :lines: 194-208
   :language: cpp

.. rubric:: createUserData()

The :ref:`createUserData() <createuserdata>` method() has two parameters, a pointer reference to userData, and a length. Both of these are [OUT] parameters.

The userData parameter is to be set to an instance of your new data structure.

The length field isn't very important in the Complex Data Model, as the data structure us usually of variable size.

This function may be called many times from different modules. Do not expect your data to be always contained in the same physical space or consolodated together. For instance, each PM will allocate memory separately for each GROUP in a GROUP BY clause. The UM will also allocate at least one instance to handle final aggregation. 

The implementation of the createUserData() method() in MEDIAN:

.. literalinclude:: ../../../median.cpp
   :lines: 187-192
   :language: cpp

