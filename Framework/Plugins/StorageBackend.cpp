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

#include <cassert>
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
  void StorageBackend::SetDatabase(IDatabase* database)
  {
    if (database == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else if (manager_.get() != NULL)
    {
      delete database;
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      manager_.reset(new DatabaseManager(database));
    }
  }
  
  DatabaseManager& StorageBackend::GetManager()
  {
    if (manager_.get() == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      return *manager_;
    }
  }
    

  void StorageBackend::Accessor::Create(const std::string& uuid,
                                        const void* content,
                                        size_t size,
                                        OrthancPluginContentType type)
  {
    DatabaseManager::Transaction transaction(manager_, TransactionType_ReadWrite);

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager_,
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

    transaction.Commit();
  }


  void StorageBackend::Accessor::Read(StorageBackend::IFileContentVisitor& visitor,
                                      const std::string& uuid,
                                      OrthancPluginContentType type) 
  {
    DatabaseManager::Transaction transaction(manager_, TransactionType_ReadOnly);

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager_,
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
            visitor.Assign(dynamic_cast<const FileValue&>(value).GetContent());
            break;

          case ValueType_BinaryString:
            visitor.Assign(dynamic_cast<const BinaryStringValue&>(value).GetContent());
            break;

          default:
            throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);        
        }
      }
    }

    transaction.Commit();

    if (!visitor.IsSuccess())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database, "Could not read attachment from the storage area");
    }
  }


  void StorageBackend::Accessor::Remove(const std::string& uuid,
                                        OrthancPluginContentType type)
  {
    DatabaseManager::Transaction transaction(manager_, TransactionType_ReadWrite);

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager_,
        "DELETE FROM StorageArea WHERE uuid=${uuid} AND type=${type}");
     
      statement.SetParameterType("uuid", ValueType_Utf8String);
      statement.SetParameterType("type", ValueType_Integer64);

      Dictionary args;
      args.SetUtf8Value("uuid", uuid);
      args.SetIntegerValue("type", type);
     
      statement.Execute(args);
    }
      
    transaction.Commit();
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
      if (backend_.get() == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      else
      {
        StorageBackend::Accessor accessor(*backend_);
        accessor.Create(uuid, content, static_cast<size_t>(size), type);
        return OrthancPluginErrorCode_Success;
      }
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


#if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 0)
  static OrthancPluginErrorCode StorageReadWhole(OrthancPluginMemoryBuffer64* target,
                                                 const char* uuid,
                                                 OrthancPluginContentType type)
  {
    class Visitor : public StorageBackend::IFileContentVisitor
    {
    private:
      OrthancPluginMemoryBuffer64* target_;
      bool                         success_;
      
    public:
      Visitor(OrthancPluginMemoryBuffer64* target) :
        target_(target),
        success_(false)
      {
      }

      virtual bool IsSuccess() const ORTHANC_OVERRIDE
      {
        return success_;
      }
      
      virtual void Assign(const std::string& content) ORTHANC_OVERRIDE
      {
        if (success_)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
        }
        else
        {
          assert(context_ != NULL);
          
          if (OrthancPluginCreateMemoryBuffer64(context_, target_, static_cast<uint64_t>(content.size())) !=
              OrthancPluginErrorCode_Success)
          {
            throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory);
          }

          if (!content.empty())
          {
            memcpy(target_->data, content.c_str(), content.size());
          }

          success_ = true;
        }
      }
    };
    
    try
    {
      if (backend_.get() == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      else if (target == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
      }
      else
      {
        Visitor visitor(target);

        {
          StorageBackend::Accessor accessor(*backend_);
          accessor.Read(visitor, uuid, type);
        }

        return OrthancPluginErrorCode_Success;
      }
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
#endif

  
  static OrthancPluginErrorCode StorageRead(void** data,
                                            int64_t* size,
                                            const char* uuid,
                                            OrthancPluginContentType type)
  {
    class Visitor : public StorageBackend::IFileContentVisitor
    {
    private:
      void**    data_;
      int64_t*  size_;
      bool      success_;
      
    public:
      Visitor(void** data,
              int64_t* size) :
        data_(data),
        size_(size),
        success_(false)
      {
      }

      virtual bool IsSuccess() const ORTHANC_OVERRIDE
      {
        return success_;
      }
      
      virtual void Assign(const std::string& content) ORTHANC_OVERRIDE
      {
        if (success_)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
        }
        else
        {
          if (content.empty())
          {
            *data_ = NULL;
            *size_ = 0;
          }
          else
          {
            *size_ = static_cast<int64_t>(content.size());
    
            if (static_cast<size_t>(*size_) != content.size())
            {
              throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory,
                                              "File cannot be stored in a 63bit buffer");
            }

            *data_ = malloc(*size_);
            if (*data_ == NULL)
            {
              throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory);
            }

            memcpy(*data_, content.c_str(), *size_);
          }
          
          success_ = true;
        }
      }
    };


    try
    {
      if (backend_.get() == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      else if (data == NULL ||
               size == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
      }
      else
      {
        Visitor visitor(data, size);

        {
          StorageBackend::Accessor accessor(*backend_);
          accessor.Read(visitor, uuid, type);
        }

        return OrthancPluginErrorCode_Success;
      }
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode StorageRemove(const char* uuid,
                                              OrthancPluginContentType type)
  {
    try
    {
      if (backend_.get() == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      else
      {
        StorageBackend::Accessor accessor(*backend_);
        accessor.Remove(uuid, type);
        return OrthancPluginErrorCode_Success;
      }
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

      bool hasLoadedV2 = false;

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 0)
      if (OrthancPluginCheckVersionAdvanced(context, 1, 9, 0) == 1)
      {
        OrthancPluginRegisterStorageArea2(context_, StorageCreate, StorageReadWhole,
                                          NULL /* TODO - StorageReadRange */, StorageRemove);
        hasLoadedV2 = true;
      }
#  endif
#endif

      if (!hasLoadedV2)
      {
        OrthancPluginRegisterStorageArea(context_, StorageCreate, StorageRead, StorageRemove);
      }
    }
  }


  void StorageBackend::Finalize()
  {
    backend_.reset(NULL);
    context_ = NULL;
  }


  void StorageBackend::Accessor::ReadToString(std::string& target,
                                              const std::string& uuid,
                                              OrthancPluginContentType type)
  {
    class Visitor : public StorageBackend::IFileContentVisitor
    {
    private:
      std::string&  target_;
      bool          success_;
      
    public:
      Visitor(std::string& target) :
        target_(target),
        success_(false)
      {
      }

      virtual bool IsSuccess() const ORTHANC_OVERRIDE
      {
        return success_;
      }
      
      virtual void Assign(const std::string& content) ORTHANC_OVERRIDE
      {
        if (success_)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
        }
        else
        {
          target_.assign(content);
          success_ = true;
        }
      }
    };
    

    Visitor visitor(target);
    Read(visitor, uuid, type);
  }
}
