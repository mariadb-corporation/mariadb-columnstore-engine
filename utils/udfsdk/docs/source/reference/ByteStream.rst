.. _bytestream:

ByteStream
==========

ByteStream is  class in Columnstore used to stream objects for transmission over TCP. It contains a binary buffer and set of methods and operators for streaming data into the buffer.

Bytestream is compiled and lives in the messageqcpp shared library. The header can be found in the Columnstore engine source tree at utils/messageqcpp/bytestream.h

#include "bytestream.h"

For UDA(n)F you should not have to instantiate a ByteStream. However, if you implement :ref:`complexdatamodel`, you will need to use the instances passed to your serialize() and unserialize() methods.

These typedefs exist only for backwards compatibility and can be ignored:

* typedef uint8_t  byte;
* typedef uint16_t doublebyte;
* typedef uint32_t quadbyte;
* typedef uint64_t octbyte;
* typedef boost::uuids::uuid uuid;

uuid may be a useful short cut if you need to stream a boost uuid, but otherwise, use the C++ standard int8_t, int16_t, etc.

.. rubric:: Buffer Allocation

ByteStream reallocates as necessary. To lower the number of allocations required, you can use the needAtLeast(size_t) method to preallocate your minimum needs.

.. rubric:: Basic Streaming

For each basic data type of int8_t, int16_t, int32_t, int64_t, float, double, uuid, std::string and the unsigned integer counterparts, there are streaming operators '<<' and '>>' defined.

In addition, if a class is derived from serializable, it can be streamed in toto with the '<<' and '>>' operators.

There are also peek() methods for each data type to allow you to look at the top of the stream without removing the value::

 int32_t val;
 void peek(val);
  will put the top 4 bytes, interpreted as an int32_t into val and leave the data in the stream.

This works for std::string::

 std::string str;
 void peek(str);

To put a binary buffer into the stream, use append. It is usually wise to put the length of the binary data into the astream before the binary data so you can get the length when unserializing::

 bs << len;  
 bs.append(reinterpret_cast<const uint8_t*>(mybuf), len);


To get binary data out of the stream, copy the data out and advance() the buffer::

 bs >> len;   
 memcpy(mybuf, bs.buf(), len); // TODO: Be sure mybuf is big enough
 bs.advance(len);

.. rubric:: Utility funcions

To determine if there's data in the stream::

 bs.empty();

To determine the raw length of the data in the stream::

 bs.length();

If for some reason, you want to empty the buffer, say if some unlikely logic causes your serialize() method to follow a new streaming logic::

 bs.restart();

To get back to the beginning while unserializing::

 bs.rewind();

And finally, to just start over, releasing the allocated buffer and allocating a new one::

 bs.reset();



