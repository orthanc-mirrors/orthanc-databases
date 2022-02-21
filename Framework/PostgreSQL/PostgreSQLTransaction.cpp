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


#include "PostgreSQLTransaction.h"

#include "PostgreSQLStatement.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>

namespace OrthancDatabases
{
  PostgreSQLTransaction::PostgreSQLTransaction(PostgreSQLDatabase& database,
                                               TransactionType type) :
    database_(database),
    isOpen_(false)
  {
    Begin(type);
  }


  PostgreSQLTransaction::~PostgreSQLTransaction()
  {
    if (isOpen_)
    {
      LOG(INFO) << "PostgreSQL: An active PostgreSQL transaction was dismissed";

      try
      {
        database_.ExecuteMultiLines("ABORT");
      }
      catch (Orthanc::OrthancException&)
      {
        // Ignore possible exceptions due to connection loss
      }
    }
  }


  void PostgreSQLTransaction::Begin(TransactionType type)
  {
    if (isOpen_) 
    {
      LOG(ERROR) << "PostgreSQL: Beginning a transaction twice!";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    database_.ExecuteMultiLines("BEGIN");

    switch (type)
    {
      case TransactionType_ReadWrite:
        database_.ExecuteMultiLines("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE");
        break;

      case TransactionType_ReadOnly:
        database_.ExecuteMultiLines("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
        
    isOpen_ = true;
  }


  void PostgreSQLTransaction::Rollback() 
  {
    if (!isOpen_) 
    {
      LOG(ERROR) << "PostgreSQL: Attempting to rollback a nonexistent transaction. "
                 << "Did you remember to call Begin()?";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    database_.ExecuteMultiLines("ABORT");
    isOpen_ = false;
  }


  void PostgreSQLTransaction::Commit() 
  {
    if (!isOpen_) 
    {
      LOG(ERROR) << "PostgreSQL: Attempting to roll back a nonexistent transaction. "
                 << "Did you remember to call Begin()?";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    database_.ExecuteMultiLines("COMMIT");
    isOpen_ = false;
  }


  IResult* PostgreSQLTransaction::Execute(IPrecompiledStatement& statement,
                                          const Dictionary& parameters)
  {
    return dynamic_cast<PostgreSQLStatement&>(statement).Execute(*this, parameters);
  }


  void PostgreSQLTransaction::ExecuteWithoutResult(IPrecompiledStatement& statement,
                                                   const Dictionary& parameters)
  {
    dynamic_cast<PostgreSQLStatement&>(statement).ExecuteWithoutResult(*this, parameters);
  }
}
