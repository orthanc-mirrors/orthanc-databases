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


#include "SQLiteTransaction.h"

#include "SQLiteResult.h"
#include "SQLiteStatement.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <OrthancException.h>

namespace OrthancDatabases
{
  SQLiteTransaction::SQLiteTransaction(SQLiteDatabase& database) :
    database_(database),
    transaction_(database.GetObject())
  {
    transaction_.Begin();

    if (!transaction_.IsOpen())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }

  IResult* SQLiteTransaction::Execute(IPrecompiledStatement& statement,
                                      const Dictionary& parameters)
  {
    return dynamic_cast<SQLiteStatement&>(statement).Execute(*this, parameters);
  }

  void SQLiteTransaction::ExecuteWithoutResult(IPrecompiledStatement& statement,
                                               const Dictionary& parameters)
  {
    dynamic_cast<SQLiteStatement&>(statement).ExecuteWithoutResult(*this, parameters);
  }
}
