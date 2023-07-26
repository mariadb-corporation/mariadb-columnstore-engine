#include "compileoperator.h"
#include "llvm/IR/Verifier.h"
#include "rowgroup.h"

namespace execplan
{
// TODO: change row to llvm param
void compileOperator(llvm::Module& module, const execplan::SRCP& expression, rowgroup::Row& row, bool& isNull)
{
  auto columns = expression.get()->simpleColumnList();
  llvm::IRBuilder<> b(module.getContext());
  llvm::Type* return_type;
  auto* data_type = b.getInt8Ty()->getPointerTo();
  auto* isNull_type = b.getInt1Ty()->getPointerTo();
  switch (expression->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::TINYINT: return_type = b.getInt64Ty(); break;
    default: throw std::runtime_error("not support type");
  }
  auto* func_type = llvm::FunctionType::get(return_type, {data_type, isNull_type}, false);
  auto* func =
      llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expression->alias(), module);

  auto* args = func->args().begin();
  llvm::Value* data_ptr = args++;
  llvm::Value* isNull_ptr = args++;
  auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
  b.SetInsertPoint(entry);
  auto* ret = expression->compile(b, data_ptr, isNull_ptr, row);

  b.CreateRet(ret);

  // Print the IR
  llvm::verifyFunction(*func);
  module.print(llvm::outs(), nullptr);
}

CompiledOperatorINT64 compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row,
                                      bool& isNull)
{
  auto compiled_module =
      jit.compileModule([&](llvm::Module& module) { compileOperator(module, expression, row, isNull); });
  auto compiled_function_ptr = reinterpret_cast<JITCompiledOperatorINT64>(
      compiled_module.function_name_to_symbol[expression->alias()]);
  CompiledOperatorINT64 result_compiled_function{.compiled_module = compiled_module,
                                                 .compiled_function = compiled_function_ptr};

  return result_compiled_function;
}

}  // namespace execplan