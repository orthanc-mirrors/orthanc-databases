/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2022 Osimis S.A., Belgium
 * Copyright (C) 2021-2022 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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

#include "../Common/ITransaction.h"
#include "SQLiteDatabase.h"

#include <SQLite/Transaction.h>

namespace OrthancDatabases
{
  class SQLiteTransaction : public ITransaction
  {
  private:
    SQLiteDatabase&               database_;
    Orthanc::SQLite::Transaction  transaction_;
    
  public:
    explicit SQLiteTransaction(SQLiteDatabase& database);

    virtual bool IsImplicit() const ORTHANC_OVERRIDE
    {
      return false;
    }
    
    virtual void Rollback() ORTHANC_OVERRIDE
    {
      transaction_.Rollback();
    }      
    
    virtual void Commit() ORTHANC_OVERRIDE
    {
      transaction_.Commit();
    }

    virtual IResult* Execute(IPrecompiledStatement& statement,
                             const Dictionary& parameters) ORTHANC_OVERRIDE;

    virtual void ExecuteWithoutResult(IPrecompiledStatement& statement,
                                      const Dictionary& parameters) ORTHANC_OVERRIDE;

    virtual bool DoesTableExist(const std::string& name) ORTHANC_OVERRIDE
    {
      return database_.GetObject().DoesTableExist(name.c_str());
    }

    virtual bool DoesTriggerExist(const std::string& name) ORTHANC_OVERRIDE
    {
      return false;
    }

    virtual void ExecuteMultiLines(const std::string& query) ORTHANC_OVERRIDE
    {
      database_.GetObject().Execute(query);
    }
  };
}
