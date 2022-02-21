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

#if ORTHANC_ENABLE_POSTGRESQL != 1
#  error PostgreSQL support must be enabled to use this file
#endif

#include "PostgreSQLParameters.h"
#include "../Common/IDatabaseFactory.h"

namespace OrthancDatabases
{
  class PostgreSQLDatabase : public IDatabase
  {
  private:
    friend class PostgreSQLStatement;
    friend class PostgreSQLLargeObject;

    class Factory;

    PostgreSQLParameters  parameters_;
    void*                 pg_;   /* Object of type "PGconn*" */

    void ThrowException(bool log);

    void Close();

    bool RunAdvisoryLockStatement(const std::string& statement);

  public:
    explicit PostgreSQLDatabase(const PostgreSQLParameters& parameters) :
      parameters_(parameters),
      pg_(NULL)
    {
    }

    ~PostgreSQLDatabase();

    void Open();

    bool AcquireAdvisoryLock(int32_t lock);

    bool ReleaseAdvisoryLock(int32_t lock);

    void AdvisoryLock(int32_t lock);

    void ExecuteMultiLines(const std::string& sql);

    bool DoesTableExist(const std::string& name);

    bool DoesColumnExist(const std::string& tableName,
                         const std::string& columnName);

    void ClearAll();   // Only for unit tests!

    virtual Dialect GetDialect() const ORTHANC_OVERRIDE
    {
      return Dialect_PostgreSQL;
    }

    virtual IPrecompiledStatement* Compile(const Query& query) ORTHANC_OVERRIDE;

    virtual ITransaction* CreateTransaction(TransactionType type) ORTHANC_OVERRIDE;

    class TransientAdvisoryLock
    {
    private:
      PostgreSQLDatabase&  database_;
      int32_t              lock_;

    public:
      TransientAdvisoryLock(PostgreSQLDatabase&  database,
                            int32_t lock);

      ~TransientAdvisoryLock();
    };

    static IDatabaseFactory* CreateDatabaseFactory(const PostgreSQLParameters& parameters);

    static PostgreSQLDatabase* CreateDatabaseConnection(const PostgreSQLParameters& parameters);
  };
}
