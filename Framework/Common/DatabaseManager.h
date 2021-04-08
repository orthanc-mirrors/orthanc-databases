/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2021 Osimis S.A., Belgium
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

#include "IDatabase.h"
#include "StatementLocation.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Enumerations.h>

#include <memory>


namespace OrthancDatabases
{
  /**
   * WARNING: In PostgreSQL releases <= 3.3 and in MySQL releases <=
   * 3.0, this class was protected by a mutex. It is now assumed that
   * locking must be implemented at a higher level.
   *
   * This class maintains a list of precompiled statements. At any
   * time, this class handles 0 or 1 active transaction.
   **/
  class DatabaseManager : public boost::noncopyable
  {
  private:
    typedef std::map<StatementLocation, IPrecompiledStatement*>  CachedStatements;

    std::unique_ptr<IDatabase>     database_;
    std::unique_ptr<ITransaction>  transaction_;
    CachedStatements               cachedStatements_;
    Dialect                        dialect_;

    void CloseIfUnavailable(Orthanc::ErrorCode e);

    IPrecompiledStatement* LookupCachedStatement(const StatementLocation& location) const;

    IPrecompiledStatement& CacheStatement(const StatementLocation& location,
                                          const Query& query);

    ITransaction& GetTransaction();

    void ReleaseImplicitTransaction();

  public:
    explicit DatabaseManager(IDatabase* database);  // Takes ownership
    
    ~DatabaseManager()
    {
      Close();
    }

    IDatabase& GetDatabase()
    {
      return *database_;
    }

    Dialect GetDialect() const
    {
      return dialect_;
    }

    void Close();
    
    void StartTransaction(TransactionType type);

    void CommitTransaction();
    
    void RollbackTransaction();


    // This class is only used in the "StorageBackend"
    class Transaction : public boost::noncopyable
    {
    private:
      DatabaseManager&  manager_;
      IDatabase&        database_;
      bool              committed_;

    public:
      explicit Transaction(DatabaseManager& manager,
                           TransactionType type);

      ~Transaction();

      void Commit();

      DatabaseManager& GetManager()
      {
        return manager_;
      }

      IDatabase& GetDatabase()
      {
        return database_;
      }

      bool DoesTableExist(const std::string& name)
      {
        return manager_.GetTransaction().DoesTableExist(name);
      }

      bool DoesTriggerExist(const std::string& name)
      {
        return manager_.GetTransaction().DoesTriggerExist(name);
      }

      void ExecuteMultiLines(const std::string& sql)
      {
        manager_.GetTransaction().ExecuteMultiLines(sql);
      }
    };


    class StatementBase : public boost::noncopyable
    {
    private:
      DatabaseManager&          manager_;
      ITransaction&             transaction_;
      std::unique_ptr<Query>    query_;
      std::unique_ptr<IResult>  result_;

      IResult& GetResult() const;

    protected:
      DatabaseManager& GetManager() const
      {
        return manager_;
      }

      ITransaction& GetTransaction() const
      {
        return transaction_;
      }
      
      void SetQuery(Query* query);

      void SetResult(IResult* result);

      void ClearResult()
      {
        result_.reset();
      }

      Query* ReleaseQuery()
      {
        return query_.release();
      }

    public:
      explicit StatementBase(DatabaseManager& manager);

      virtual ~StatementBase();

      // Used only by SQLite
      IDatabase& GetDatabase()
      {
        return manager_.GetDatabase();
      }

      void SetReadOnly(bool readOnly);

      void SetParameterType(const std::string& parameter,
                            ValueType type);
      
      bool IsDone() const;
      
      void Next();

      size_t GetResultFieldsCount() const;

      void SetResultFieldType(size_t field,
                              ValueType type);
      
      const IValue& GetResultField(size_t index) const;
    };


    class CachedStatement : public StatementBase
    {
    private:
      StatementLocation       location_;
      IPrecompiledStatement*  statement_;

    public:
      CachedStatement(const StatementLocation& location,
                      DatabaseManager& manager,
                      const std::string& sql);

      void Execute()
      {
        Dictionary parameters;
        Execute(parameters);
      }

      void Execute(const Dictionary& parameters);
    };


    class StandaloneStatement : public StatementBase
    {
    private:
      std::unique_ptr<IPrecompiledStatement>  statement_;
      
    public:
      StandaloneStatement(DatabaseManager& manager,
                          const std::string& sql);

      virtual ~StandaloneStatement();

      void Execute()
      {
        Dictionary parameters;
        Execute(parameters);
      }

      void Execute(const Dictionary& parameters);
    };
  };
}
