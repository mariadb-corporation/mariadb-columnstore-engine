#include "llvm/IR/Value.h"
#include "llvm/IR/IRBuilder.h"
#include "datatypes/mcs_datatype.h"

using namespace execplan;
namespace msc_jit
{
class CompileHelper
{
 public:
  template <int len>
  static llvm::Value* compileIntField(llvm::IRBuilder<>& b, llvm::Value* dataValue, uint32_t offset,
                                      bool isInt64 = true)
  {
    auto* dataPtr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), dataValue, offset);
    llvm::Value* result;
    switch (len)
    {
      case 1: result = b.CreateLoad(b.getInt8Ty(), dataPtr); break;
      case 2:
        result = b.CreateLoad(b.getInt16Ty(), b.CreateBitCast(dataPtr, b.getInt16Ty()->getPointerTo()));
        break;
      case 4:
        result = b.CreateLoad(b.getInt32Ty(), b.CreateBitCast(dataPtr, b.getInt32Ty()->getPointerTo()));
        break;
      case 8:
        result = b.CreateLoad(b.getInt64Ty(), b.CreateBitCast(dataPtr, b.getInt64Ty()->getPointerTo()));
        break;
      default: throw std::logic_error("Row::compileIntField(): bad length.");
    }
    return isInt64 ? b.CreateSExt(result, b.getInt64Ty()) : result;
  }

  template <int len>
  static llvm::Value* compileUintField(llvm::IRBuilder<>& b, llvm::Value* dataValue, uint32_t offset,
                                       bool isUint64 = true)
  {
    auto* dataPtr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), dataValue, offset);
    llvm::Value* result;
    switch (len)
    {
      case 1: result = b.CreateLoad(b.getInt8Ty(), dataPtr); break;
      case 2:
        result = b.CreateLoad(b.getInt16Ty(), b.CreateBitCast(dataPtr, b.getInt16Ty()->getPointerTo()));
        break;
      case 4:
        result = b.CreateLoad(b.getInt32Ty(), b.CreateBitCast(dataPtr, b.getInt32Ty()->getPointerTo()));
        break;
      case 8:
        result = b.CreateLoad(b.getInt64Ty(), b.CreateBitCast(dataPtr, b.getInt64Ty()->getPointerTo()));
        break;
      default: throw std::logic_error("Row::compileIntField(): bad length.");
    }
    return isUint64 ? b.CreateZExt(result, b.getInt64Ty()) : result;
  }

  static llvm::Value* compileFloatField(llvm::IRBuilder<>& b, llvm::Value* dataValue, uint32_t offset)
  {
    auto* dataPtr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), dataValue, offset);
    return b.CreateLoad(b.getFloatTy(), b.CreateBitCast(dataPtr, b.getFloatTy()->getPointerTo()));
  }

  static llvm::Value* compileDoubleField(llvm::IRBuilder<>& b, llvm::Value* dataValue, uint32_t offset)
  {
    auto* dataPtr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), dataValue, offset);
    return b.CreateLoad(b.getDoubleTy(), b.CreateBitCast(dataPtr, b.getDoubleTy()->getPointerTo()));
  }

  static void compileIsNull(llvm::IRBuilder<>& b, llvm::Value* dataValue, llvm::Value* isNull,
                            uint32_t offset, CalpontSystemCatalog::ColDataType& dataType)
  {
    auto* isNullVal = b.CreateLoad(b.getInt1Ty(), isNull);
    switch (dataType)
    {
      case CalpontSystemCatalog::TINYINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<1>(b, dataValue, offset, false),
                                                b.getInt8(joblist::TINYINTNULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::SMALLINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<2>(b, dataValue, offset, false),
                                                b.getInt16(static_cast<int16_t>(joblist::SMALLINTNULL))),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<4>(b, dataValue, offset, false),
                                                b.getInt32(static_cast<int32_t>(joblist::INTNULL))),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<4>(b, dataValue, offset, false),
                                                b.getInt32(static_cast<int32_t>(joblist::FLOATNULL))),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::DATE:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<4>(b, dataValue, offset, false),
                                                b.getInt32(static_cast<int32_t>(joblist::DATENULL))),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::BIGINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<8>(b, dataValue, offset, false),
                                                b.getInt64(static_cast<int64_t>(joblist::BIGINTNULL))),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::DOUBLE:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<8>(b, dataValue, offset, false),
                                                b.getInt64(joblist::DOUBLENULL)),
                                 isNullVal),
                      isNull);
      case CalpontSystemCatalog::UDOUBLE:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileUintField<8>(b, dataValue, offset, false),
                                                b.getInt64(joblist::DOUBLENULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::DATETIME:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileIntField<8>(b, dataValue, offset, false),
                                                b.getInt64(joblist::DATETIMENULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::UTINYINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileUintField<1>(b, dataValue, offset, false),
                                                b.getInt8(joblist::UTINYINTNULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::USMALLINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileUintField<2>(b, dataValue, offset, false),
                                                b.getInt16(joblist::USMALLINTNULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileUintField<4>(b, dataValue, offset, false),
                                                b.getInt32(joblist::UINTNULL)),
                                 isNullVal),
                      isNull);
        break;
      case CalpontSystemCatalog::UBIGINT:
        b.CreateStore(b.CreateOr(b.CreateICmpEQ(compileUintField<8>(b, dataValue, offset, false),
                                                b.getInt64(joblist::UBIGINTNULL)),
                                 isNullVal),
                      isNull);
        break;
      default:
      {
        ostringstream os;
        os << "Row::compileIsNull(): got bad column type (";
        os << dataType;
        os << ").  ";
        throw logic_error(os.str());
      }
    }
  }
};

}  // namespace msc_jit
