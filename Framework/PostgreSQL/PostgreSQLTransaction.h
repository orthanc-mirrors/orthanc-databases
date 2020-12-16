/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2020 Osimis S.A., Belgium
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

#if ORTHANC_ENABLE_POSTGRESQL != 1
#  error PostgreSQL support must be enabled to use this file
#endif

#include "../Common/ITransaction.h"

#include "PostgreSQLDatabase.h"

namespace OrthancDatabases
{
  class PostgreSQLTransaction : public ITransaction
  {
  private:
    PostgreSQLDatabase& database_;
    bool isOpen_;
    bool readOnly_;

  public:
    explicit PostgreSQLTransaction(PostgreSQLDatabase& database);

    ~PostgreSQLTransaction();

    virtual bool IsImplicit() const ORTHANC_OVERRIDE
    {
      return false;
    }
    
    virtual bool IsReadOnly() const  ORTHANC_OVERRIDE
    {
      return readOnly_;
    }

    void Begin();

    virtual void Rollback() ORTHANC_OVERRIDE;

    virtual void Commit() ORTHANC_OVERRIDE;

    virtual IResult* Execute(IPrecompiledStatement& statement,
                             const Dictionary& parameters) ORTHANC_OVERRIDE;

    virtual void ExecuteWithoutResult(IPrecompiledStatement& statement,
                                      const Dictionary& parameters) ORTHANC_OVERRIDE;
  };
}
