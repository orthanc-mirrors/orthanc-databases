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


#include "DatabaseManager.h"

#include "../../Resources/Orthanc/Plugins/OrthancPluginCppWrapper.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>

#include <boost/thread.hpp>

namespace OrthancDatabases
{
  void DatabaseManager::Close()
  {
    LOG(TRACE) << "Closing the connection to the database";

    // Rollback active transaction, if any
    transaction_.reset(NULL);

    // Delete all the cached statements (must occur before closing
    // the database)
    for (CachedStatements::iterator it = cachedStatements_.begin();
         it != cachedStatements_.end(); ++it)
    {
      assert(it->second != NULL);
      delete it->second;
    }

    cachedStatements_.clear();

    // Close the database
    database_.reset(NULL);

    LOG(TRACE) << "Connection to the database is closed";
  }

    
  void DatabaseManager::CloseIfUnavailable(Orthanc::ErrorCode e)
  {
    bool failure;

#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 2)
    failure = (e != Orthanc::ErrorCode_Success &&
               e != Orthanc::ErrorCode_DatabaseCannotSerialize);
#else
    failure = (e != Orthanc::ErrorCode_Success);
#endif

    if (failure)
    {
      transaction_.reset(NULL);
    }

    if (e == Orthanc::ErrorCode_DatabaseUnavailable)
    {
      LOG(ERROR) << "The database is not available, closing the connection";
      Close();
    }
  }


  IPrecompiledStatement* DatabaseManager::LookupCachedStatement(const StatementLocation& location) const
  {
    CachedStatements::const_iterator found = cachedStatements_.find(location);

    if (found == cachedStatements_.end())
    {
      return NULL;
    }
    else
    {
      assert(found->second != NULL);
      return found->second;
    }
  }

    
  IPrecompiledStatement& DatabaseManager::CacheStatement(const StatementLocation& location,
                                                         const Query& query)
  {
    LOG(TRACE) << "Caching statement from " << location.GetFile() << ":" << location.GetLine();
      
    std::unique_ptr<IPrecompiledStatement> statement(GetDatabase().Compile(query));
      
    IPrecompiledStatement* tmp = statement.get();
    if (tmp == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }

    assert(cachedStatements_.find(location) == cachedStatements_.end());
    cachedStatements_[location] = statement.release();

    return *tmp;
  }

    
  ITransaction& DatabaseManager::GetTransaction()
  {
    if (transaction_.get() == NULL)
    {
      LOG(TRACE) << "Automatically creating an implicit database transaction";

      try
      {
        transaction_.reset(GetDatabase().CreateTransaction(TransactionType_Implicit));
      }
      catch (Orthanc::OrthancException& e)
      {
        CloseIfUnavailable(e.GetErrorCode());
        throw;
      }
    }

    assert(transaction_.get() != NULL);
    return *transaction_;
  }


  void DatabaseManager::ReleaseImplicitTransaction()
  {
    if (transaction_.get() != NULL &&
        transaction_->IsImplicit())
    {
      LOG(TRACE) << "Committing an implicit database transaction";

      try
      {
        transaction_->Commit();
        transaction_.reset(NULL);
      }
      catch (Orthanc::OrthancException& e)
      {
        // Don't throw the exception, as we are in CachedStatement destructor
        LOG(ERROR) << "Error while committing an implicit database transaction: " << e.What();
      }
    }
  }

    
  DatabaseManager::DatabaseManager(IDatabase* database) :
    database_(database)
  {
    if (database == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }

    dialect_ = database->GetDialect();
  }

  
  void DatabaseManager::StartTransaction(TransactionType type)
  {
    try
    {
      if (transaction_.get() != NULL)
      {
        LOG(ERROR) << "Cannot start another transaction while there is an uncommitted transaction";
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
      }

      transaction_.reset(GetDatabase().CreateTransaction(type));
    }
    catch (Orthanc::OrthancException& e)
    {
      CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }
  

  void DatabaseManager::CommitTransaction()
  {
    if (transaction_.get() == NULL)
    {
      LOG(ERROR) << "Cannot commit a non-existing transaction";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      try
      {
        transaction_->Commit();
        transaction_.reset(NULL);
      }
      catch (Orthanc::OrthancException& e)
      {
        CloseIfUnavailable(e.GetErrorCode());
        throw;
      }
    }
  }


  void DatabaseManager::RollbackTransaction()
  {
    if (transaction_.get() == NULL)
    {
      LOG(INFO) << "Cannot rollback a non-existing transaction";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      try
      {
        transaction_->Rollback();
        transaction_.reset(NULL);
      }
      catch (Orthanc::OrthancException& e)
      {
        CloseIfUnavailable(e.GetErrorCode());
        throw;
      }
    }
  }


  DatabaseManager::Transaction::Transaction(DatabaseManager& manager,
                                            TransactionType type) :
    manager_(manager),
    database_(manager.GetDatabase()),
    active_(true)
  {
    manager_.StartTransaction(type);
  }


  DatabaseManager::Transaction::~Transaction()
  {
    if (active_)
    {
      try
      {
        manager_.RollbackTransaction();
      }
      catch (Orthanc::OrthancException& e)
      {
        // Don't rethrow the exception as we are in a destructor
        LOG(ERROR) << "Uncatched error during some transaction rollback: " << e.What();
      }
    }
  }

  
  void DatabaseManager::Transaction::Commit()
  {
    if (active_)
    {
      manager_.CommitTransaction();
      active_ = true;
    }
    else
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
  }


  void DatabaseManager::Transaction::Rollback()
  {
    if (active_)
    {
      manager_.RollbackTransaction();
      active_ = true;
    }
    else
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
  }


  IResult& DatabaseManager::StatementBase::GetResult() const
  {
    if (result_.get() == NULL)
    {
      LOG(ERROR) << "Accessing the results of a statement without having executed it";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    return *result_;
  }


  void DatabaseManager::StatementBase::SetQuery(Query* query)
  {
    std::unique_ptr<Query> protection(query);
    
    if (query_.get() != NULL)
    {
      LOG(ERROR) << "Cannot set twice a query";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    if (query == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }

    query_.reset(protection.release());
  }

  
  void DatabaseManager::StatementBase::SetResult(IResult* result)
  {
    std::unique_ptr<IResult> protection(result);
    
    if (result_.get() != NULL)
    {
      LOG(ERROR) << "Cannot execute twice a statement";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    if (result == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }

    result_.reset(protection.release());
  }

  
  DatabaseManager::StatementBase::StatementBase(DatabaseManager& manager) :
    manager_(manager),
    transaction_(manager_.GetTransaction())
  {
  }


  DatabaseManager::StatementBase::~StatementBase()
  {
    manager_.ReleaseImplicitTransaction();
  }

  
  void DatabaseManager::StatementBase::SetReadOnly(bool readOnly)
  {
    if (query_.get() != NULL)
    {
      query_->SetReadOnly(readOnly);
    }
  }


  void DatabaseManager::StatementBase::SetParameterType(const std::string& parameter,
                                                        ValueType type)
  {
    if (query_.get() != NULL)
    {
      query_->SetType(parameter, type);
    }
  }
      
  bool DatabaseManager::StatementBase::IsDone() const
  {
    try
    {
      return GetResult().IsDone();
    }
    catch (Orthanc::OrthancException& e)
    {
      manager_.CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }


  void DatabaseManager::StatementBase::Next()
  {
    try
    {
      GetResult().Next();
    }
    catch (Orthanc::OrthancException& e)
    {
      manager_.CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }


  size_t DatabaseManager::StatementBase::GetResultFieldsCount() const
  {
    try
    {
      return GetResult().GetFieldsCount();
    }
    catch (Orthanc::OrthancException& e)
    {
      manager_.CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }


  void DatabaseManager::StatementBase::SetResultFieldType(size_t field,
                                                          ValueType type)
  {
    try
    {
      if (!GetResult().IsDone())
      {
        GetResult().SetExpectedType(field, type);
      }
    }
    catch (Orthanc::OrthancException& e)
    {
      manager_.CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }


  const IValue& DatabaseManager::StatementBase::GetResultField(size_t index) const
  {
    try
    {
      return GetResult().GetField(index);
    }
    catch (Orthanc::OrthancException& e)
    {
      manager_.CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }  
  
  
  DatabaseManager::CachedStatement::CachedStatement(const StatementLocation& location,
                                                    DatabaseManager& manager,
                                                    const std::string& sql) :
    StatementBase(manager),
    location_(location)
  {
    statement_ = GetManager().LookupCachedStatement(location_);

    if (statement_ == NULL)
    {
      SetQuery(new Query(sql));
    }
    else
    {
      LOG(TRACE) << "Reusing cached statement from "
                 << location_.GetFile() << ":" << location_.GetLine();
    }
  }

      
  void DatabaseManager::CachedStatement::Execute(const Dictionary& parameters)
  {
    try
    {
      std::unique_ptr<Query> query(ReleaseQuery());
      
      if (query.get() != NULL)
      {
        // Register the newly-created statement
        assert(statement_ == NULL);
        statement_ = &GetManager().CacheStatement(location_, *query);
      }
        
      assert(statement_ != NULL);

      /*
        TODO - Sample code to monitor the execution time of each
        cached statement, and publish it as an Orthanc metrics

        #if HAS_ORTHANC_PLUGIN_METRICS == 1
        std::string name = (std::string(location_.GetFile()) + "_" +
        boost::lexical_cast<std::string>(location_.GetLine()));
        OrthancPlugins::MetricsTimer timer(name.c_str());
        #endif
      */

      SetResult(GetTransaction().Execute(*statement_, parameters));
    }
    catch (Orthanc::OrthancException& e)
    {
      GetManager().CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }
  
  
  DatabaseManager::StandaloneStatement::StandaloneStatement(DatabaseManager& manager,
                                                            const std::string& sql) :
    StatementBase(manager)
  {
    SetQuery(new Query(sql));
  }

      
  DatabaseManager::StandaloneStatement::~StandaloneStatement()
  {
    // The result must be removed before the statement, cf. (*)
    ClearResult();
    statement_.reset();
  }


  void DatabaseManager::StandaloneStatement::Execute(const Dictionary& parameters)
  {
    try
    {
      std::unique_ptr<Query> query(ReleaseQuery());
      assert(query.get() != NULL);

      // The "statement_" object must be kept as long as the "IResult"
      // is not destroyed, as the "IResult" can make calls to the
      // statement (this is the case for SQLite and MySQL) - (*)
      statement_.reset(GetManager().GetDatabase().Compile(*query));
      assert(statement_.get() != NULL);

      SetResult(GetTransaction().Execute(*statement_, parameters));
    }
    catch (Orthanc::OrthancException& e)
    {
      GetManager().CloseIfUnavailable(e.GetErrorCode());
      throw;
    }
  }
}
