/* Copyright (C) 2024 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#pragma once
#include <memory>
#include "fdbcs.hpp"

namespace storagemanager
{
// Represnts a `Key/Value` storage initializer based on Singleton pattern.
// Initializes all needed `Key/Value` storage machinery once and return pointer
// to the `Key/Value` storage.
class KVStorageInitializer
{
 public:
  static std::shared_ptr<FDBCS::FDBDataBase> getStorageInstance();
  KVStorageInitializer() = delete;
  KVStorageInitializer(const KVStorageInitializer&) = delete;
  KVStorageInitializer& operator=(KVStorageInitializer&) = delete;
  KVStorageInitializer(KVStorageInitializer&&) = delete;
  KVStorageInitializer& operator=(KVStorageInitializer&&) = delete;
};

}  // namespace storagemanager
