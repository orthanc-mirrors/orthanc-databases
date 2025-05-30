/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2024 Orthanc Team SRL, Belgium
 * Copyright (C) 2021-2024 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 **/


#pragma once

#if ORTHANC_ENABLE_SQLITE != 1
#  error SQLite support must be enabled to use this file
#endif

#include "../Common/IDatabase.h"

#include <SQLite/Connection.h>

namespace OrthancDatabases
{
  class SQLiteDatabase : public IDatabase
  {
  private:
    Orthanc::SQLite::Connection  connection_;
    
  public:
    void OpenInMemory()
    {
      connection_.OpenInMemory();
    }

    void Open(const std::string& path)
    {
      connection_.Open(path);
    }
    
    Orthanc::SQLite::Connection& GetObject()
    {
      return connection_;
    }

    void Execute(const std::string& sql);
    
    int64_t GetLastInsertRowId() const
    {
      return connection_.GetLastInsertRowId();
    }
    
    virtual Dialect GetDialect() const ORTHANC_OVERRIDE
    {
      return Dialect_SQLite;
    }

    virtual IPrecompiledStatement* Compile(const Query& query) ORTHANC_OVERRIDE;

    virtual ITransaction* CreateTransaction(TransactionType type) ORTHANC_OVERRIDE;
  };
}
