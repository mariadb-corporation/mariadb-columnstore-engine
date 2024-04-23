#include "compileoperator.h"
#include "llvm/IR/Verifier.h"
#include "rowgroup.h"

namespace execplan
{

void compileExternalFunction(llvm::Module& module, llvm::IRBuilder<>& b)
{
  auto* func_intToDatetime_type =
      llvm::FunctionType::get(b.getInt64Ty(), {b.getInt64Ty(), b.getInt1Ty()->getPointerTo()}, false);
  llvm::Function::Create(func_intToDatetime_type, llvm::Function::ExternalLinkage,
                         "dataconvert::DataConvert::intToDatetime", module);
  auto* func_timestampValueToInt_type =
      llvm::FunctionType::get(b.getInt64Ty(), {b.getInt64Ty(), b.getInt64Ty()}, false);
  llvm::Function::Create(func_timestampValueToInt_type, llvm::Function::ExternalLinkage,
                         "dataconvert::DataConvert::timestampValueToInt", module);
}

std::string computeFunctionName(const execplan::SRCP& expression)
{
  return expression->toExpressionString();
}

void compileOperator(llvm::Module& module, const execplan::SRCP& expression, rowgroup::Row& row)
{
  auto columns = expression.get()->simpleColumnList();
  llvm::IRBuilder<> b(module.getContext());
  // TODO this should be compiled only if
  compileExternalFunction(module, b);
  llvm::Type* returnType;
  // These are fixed types.
  auto* dataType = b.getInt8Ty()->getPointerTo();
  auto* isNullType = b.getInt1Ty()->getPointerTo();
  auto* dataConditionErrorType = b.getInt8Ty()->getPointerTo();

  switch (expression->resultType().colDataType)
  {
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UTINYINT: returnType = b.getInt64Ty(); break;
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: returnType = b.getDoubleTy(); break;
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: returnType = b.getFloatTy(); break;
    default: throw logic_error("compileOperator: unsupported type");
  }
  const auto& functionName = computeFunctionName(expression);

  auto* funcType = llvm::FunctionType::get(returnType, {dataType, isNullType, dataConditionErrorType}, false);
  auto* func = llvm::Function::Create(funcType, llvm::Function::ExternalLinkage, functionName, module);
  func->setDoesNotThrow();
  auto* args = func->args().begin();
  llvm::Value* dataPtr = args++;
  llvm::Value* isNullPtr = args++;
  llvm::Value* dataConditionPtr = args++;

  auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
  b.SetInsertPoint(entry);
  auto* ret =
      expression->compile(b, dataPtr, isNullPtr, dataConditionPtr, row, expression->resultType().colDataType);
  b.CreateRet(ret);
  llvm::verifyFunction(*func);
}

// change this to a return type
CompiledOperator compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row)
{
  auto compiled_module =
      jit.compileModule([&](llvm::Module& module) { compileOperator(module, expression, row); });
  CompiledOperator compiledOperator{.compiled_module = compiled_module};

  const auto& functionName = computeFunctionName(expression);

  switch (expression->resultType().colDataType)
  {
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::DATE:
      compiledOperator.compiled_function_int64 = reinterpret_cast<JITCompiledOperator<int64_t>>(
          compiled_module.function_name_to_symbol[functionName]);
      break;
    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UTINYINT:
      compiledOperator.compiled_function_uint64 =
          reinterpret_cast<JITIntConditionedCompiledOperator<uint64_t>>(
              compiled_module.function_name_to_symbol[functionName]);
      break;
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
      compiledOperator.compiled_function_double = reinterpret_cast<JITCompiledOperator<double>>(
          compiled_module.function_name_to_symbol[functionName]);
      break;
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
      compiledOperator.compiled_function_float =
          reinterpret_cast<JITCompiledOperator<float>>(compiled_module.function_name_to_symbol[functionName]);
      break;
    default: throw logic_error("compileOperator: unsupported type");
  }

  return compiledOperator;
}

}  // namespace execplan