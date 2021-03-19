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


#include "StorageBackend.h"

#if HAS_ORTHANC_EXCEPTION != 1
#  error HAS_ORTHANC_EXCEPTION must be set to 1
#endif

#include "../../Framework/Common/BinaryStringValue.h"
#include "../../Framework/Common/FileValue.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <OrthancException.h>

#include <limits>


#define ORTHANC_PLUGINS_DATABASE_CATCH                                  \
  catch (::Orthanc::OrthancException& e)                                \
  {                                                                     \
    return static_cast<OrthancPluginErrorCode>(e.GetErrorCode());       \
  }                                                                     \
  catch (::std::runtime_error& e)                                       \
  {                                                                     \
    std::string s = "Exception in storage area back-end: " + std::string(e.what()); \
    OrthancPluginLogError(context_, s.c_str());                         \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }                                                                     \
  catch (...)                                                           \
  {                                                                     \
    OrthancPluginLogError(context_, "Native exception");                \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }


namespace OrthancDatabases
{
  StorageBackend::StorageBackend(IDatabaseFactory* factory) :
    manager_(factory)
  {
  }


  void StorageBackend::Create(DatabaseManager::Transaction& transaction,
                              const std::string& uuid,
                              const void* content,
                              size_t size,
                              OrthancPluginContentType type)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "INSERT INTO StorageArea VALUES (${uuid}, ${content}, ${type})");
     
    statement.SetParameterType("uuid", ValueType_Utf8String);
    statement.SetParameterType("content", ValueType_File);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetUtf8Value("uuid", uuid);
    args.SetFileValue("content", content, size);
    args.SetIntegerValue("type", type);
     
    statement.Execute(args);
  }


  void StorageBackend::Read(StorageAreaBuffer& target,
                            DatabaseManager::Transaction& transaction, 
                            const std::string& uuid,
                            OrthancPluginContentType type) 
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "SELECT content FROM StorageArea WHERE uuid=${uuid} AND type=${type}");
     
    statement.SetParameterType("uuid", ValueType_Utf8String);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetUtf8Value("uuid", uuid);
    args.SetIntegerValue("type", type);
     
    statement.Execute(args);

    if (statement.IsDone())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_UnknownResource);
    }
    else if (statement.GetResultFieldsCount() != 1)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
    }
    else
    {
      const IValue& value = statement.GetResultField(0);
      
      switch (value.GetType())
      {
        case ValueType_File:
          target.Assign(dynamic_cast<const FileValue&>(value).GetContent());
          break;

        case ValueType_BinaryString:
          target.Assign(dynamic_cast<const BinaryStringValue&>(value).GetContent());
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
      }
    }
  }


  void StorageBackend::Remove(DatabaseManager::Transaction& transaction,
                              const std::string& uuid,
                              OrthancPluginContentType type)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, GetManager(),
      "DELETE FROM StorageArea WHERE uuid=${uuid} AND type=${type}");
     
    statement.SetParameterType("uuid", ValueType_Utf8String);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetUtf8Value("uuid", uuid);
    args.SetIntegerValue("type", type);
     
    statement.Execute(args);
  }



  static OrthancPluginContext* context_ = NULL;
  static std::unique_ptr<StorageBackend>  backend_;
    

  static OrthancPluginErrorCode StorageCreate(const char* uuid,
                                              const void* content,
                                              int64_t size,
                                              OrthancPluginContentType type)
  {
    try
    {
      DatabaseManager::Transaction transaction(backend_->GetManager(), TransactionType_ReadWrite);
      backend_->Create(transaction, uuid, content, static_cast<size_t>(size), type);
      transaction.Commit();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 0)
  static OrthancPluginErrorCode StorageReadWhole(OrthancPluginMemoryBuffer64 *target,
                                                 const char* uuid,
                                                 OrthancPluginContentType type)
  {
    try
    {
      StorageAreaBuffer buffer(context_);

      {
        DatabaseManager::Transaction transaction(backend_->GetManager(), TransactionType_ReadOnly);
        backend_->Read(buffer, transaction, uuid, type);
        transaction.Commit();
      }

      buffer.Move(target);
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
#else
  static OrthancPluginErrorCode StorageRead(void** content,
                                            int64_t* size,
                                            const char* uuid,
                                            OrthancPluginContentType type)
  {
    try
    {
      StorageAreaBuffer buffer(context_);

      {
        DatabaseManager::Transaction transaction(backend_->GetManager());
        backend_->Read(buffer, transaction, uuid, type);
        transaction.Commit();
      }

      *size = buffer.GetSize();
      *content = buffer.ReleaseData();
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
#endif


  static OrthancPluginErrorCode StorageRemove(const char* uuid,
                                              OrthancPluginContentType type)
  {
    try
    {
      DatabaseManager::Transaction transaction(backend_->GetManager(), TransactionType_ReadWrite);
      backend_->Remove(transaction, uuid, type);
      transaction.Commit();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }

  
  void StorageBackend::Register(OrthancPluginContext* context,
                                StorageBackend* backend)
  {
    if (context == NULL ||
        backend == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    
    if (context_ != NULL ||
        backend_.get() != NULL)
    {
      // This function can only be invoked once in the plugin
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      context_ = context;
      backend_.reset(backend);
      backend_->GetManager().Open();

#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 0)
      OrthancPluginRegisterStorageArea2(context_, StorageCreate, StorageReadWhole,
                                        NULL /* TODO - StorageReadRange */, StorageRemove);
#else
      OrthancPluginRegisterStorageArea(context_, StorageCreate, StorageRead, StorageRemove);
#endif
    }
  }


  void StorageBackend::Finalize()
  {
    backend_.reset(NULL);
    context_ = NULL;
  }
}
