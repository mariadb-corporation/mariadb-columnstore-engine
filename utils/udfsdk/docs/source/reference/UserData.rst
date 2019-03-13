UserData
--------

The UserData struct is used for allocating and streaming the intermediate data needed by the UDA(n)F. UserData is function specific instance data. 

There may be multiple instantiations of UserData for each UDA(n)F call. Make no assumptions about the contents except within the context of a method call.

Since there is no need to deal with the UserData Structure unless you are using :ref:`complexdatamodel`, the remainder of this page assumes :ref:`complexdatamodel`.

To use :ref:`complexdatamodel`, you derive a struct from UserData and override, at the least, the streaming functions.

.. rubric:: constructor

.. c:function:: UserData();

 Constructors are called by your code, so adding other constructors is acceptable.

.. rubric:: destructor

.. c:function:: virtual ~UserData();

 Be sure to cleanup any internal data structures or containers you may have populated that don't have automatic cleanup.

.. rubric:: Streaming methods

As data is passed between processes and modules, it must be streamed. This is because complex data structures are generally not stored in sequential bytes of memory. For instance, in a std::map container, each of its elements is stored discretely wherever the OS puts them. Streaming allows us to take all those disparate data pieces and put them on the wire as one unit.

.. c:function:: virtual void serialize(messageqcpp::ByteStream& bs) const;

:param bs: A ByteStream object to which you stream the UserData values.
	
 Stream all the UserData to a ByteStream. See the section on ByteStream for the usage of that object.

.. c:function:: virtual void unserialize(messageqcpp::ByteStream& bs);

:param bs: A ByteStream object from which you stream the UserData values,

 Unstream the data from the ByteStream and put it back into the data structure. This method **must** exactly match the serialize() implementation.

