#include "compileoperator.h"
#include "llvm/IR/Verifier.h"

namespace execplan
{
// TODO: change row to llvm param
void compileOperator(llvm::Module& module, const execplan::SRCP& expression, rowgroup::Row& row, bool& isNull)
{
  auto columns = expression.get()->simpleColumnList();
  size_t column_size = columns.size();
  llvm::IRBuilder<> b(module.getContext());
  std::vector<llvm::Type*> params;
  llvm::Type* return_type;
//  for (auto column : columns)
//  {
//    switch (column->colType().colDataType)
//    {
//      case execplan::CalpontSystemCatalog::BIGINT:
//      case execplan::CalpontSystemCatalog::INT:
//      case execplan::CalpontSystemCatalog::MEDINT:
//      case execplan::CalpontSystemCatalog::SMALLINT:
//      case execplan::CalpontSystemCatalog::TINYINT:
//        params.emplace_back(b.getInt64Ty());
//        break;
//      default: throw std::runtime_error("not support type:" + std::to_string(column->colType().colDataType));
//    }
//  }

  switch (expression->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::TINYINT:
      return_type = b.getInt64Ty();
      break;
    default: throw std::runtime_error("not support type");
  }
  auto* func_type = llvm::FunctionType::get(return_type, params, false);
  auto* func =
      llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, expression->alias(), module);

  auto* entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
  b.SetInsertPoint(entry);
  auto* ret = expression->compile(b, row, isNull);

  b.CreateRet(ret);

  // Print the IR
  llvm::verifyFunction(*func);
  module.print(llvm::outs(), nullptr);
}

CompiledOperator compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row,
                                 bool& isNull)
{
  std::cout << expression->alias();
  auto compiled_module =
      jit.compileModule([&](llvm::Module& module) { compileOperator(module, expression, row, isNull); });

  CompiledOperator result_compiled_function{.compiled_module = compiled_module};

  return result_compiled_function;
}

}  // namespace execplan