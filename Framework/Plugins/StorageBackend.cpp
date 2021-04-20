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
#include "../../Framework/Common/ResultFileValue.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
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
  StorageBackend::StorageBackend(IDatabaseFactory* factory) :
    manager_(factory)
  {
  }
  
  void StorageBackend::AccessorBase::Create(const std::string& uuid,
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
      statement.SetParameterType("content", ValueType_InputFile);
      statement.SetParameterType("type", ValueType_Integer64);

      Dictionary args;
      args.SetUtf8Value("uuid", uuid);
      args.SetFileValue("content", content, size);
      args.SetIntegerValue("type", type);
     
      statement.Execute(args);
    }

    transaction.Commit();
  }


  void StorageBackend::AccessorBase::ReadWhole(StorageBackend::IFileContentVisitor& visitor,
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
          case ValueType_ResultFile:
          {
            std::string content;
            dynamic_cast<const ResultFileValue&>(value).ReadWhole(content);
            visitor.Assign(content);
            break;
          }

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


  void StorageBackend::AccessorBase::ReadRange(IFileContentVisitor& visitor,
                                               const std::string& uuid,
                                               OrthancPluginContentType type,
                                               uint64_t start,
                                               size_t length)
  {
    /**
     * This is a generic implementation, that will only work if
     * "ResultFileValue" is implemented by the database backend. For
     * instance, this will *not* work with MySQL, as the latter uses
     * BLOB columns to store files.
     **/
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
        if (value.GetType() == ValueType_ResultFile)
        {
          std::string content;
          dynamic_cast<const ResultFileValue&>(value).ReadRange(content, start, length);
          visitor.Assign(content);
        }
        else
        {
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


  void StorageBackend::AccessorBase::Remove(const std::string& uuid,
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
        std::unique_ptr<StorageBackend::IAccessor> accessor(backend_->CreateAccessor());
        accessor->Create(uuid, content, static_cast<size_t>(size), type);
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
        if (target == NULL)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
        }
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
      else
      {
        Visitor visitor(target);

        {
          std::unique_ptr<StorageBackend::IAccessor> accessor(backend_->CreateAccessor());
          accessor->ReadWhole(visitor, uuid, type);
        }

        return OrthancPluginErrorCode_Success;
      }
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }

  
  static OrthancPluginErrorCode StorageReadRange(OrthancPluginMemoryBuffer64* target,
                                                 const char* uuid,
                                                 OrthancPluginContentType type,
                                                 uint64_t start)
  {
    class Visitor : public StorageBackend::IFileContentVisitor
    {
    private:
      OrthancPluginMemoryBuffer64* target_;  // This buffer is already allocated by the Orthanc core
      bool                         success_;
      
    public:
      Visitor(OrthancPluginMemoryBuffer64* target) :
        target_(target),
        success_(false)
      {
        if (target == NULL)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
        }
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
          if (content.size() != target_->size)
          {
            throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
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
      else
      {
        Visitor visitor(target);

        {
          std::unique_ptr<StorageBackend::IAccessor> accessor(backend_->CreateAccessor());
          accessor->ReadRange(visitor, uuid, type, start, target->size);
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

      ~Visitor()
      {
        if (data_ != NULL /* this condition is invalidated by "Release()" */ &&
            *data_ != NULL)
        {
          free(*data_);
        }
      }

      void Release()
      {
        data_ = NULL;
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
        else if (data_ == NULL)
        {
          // "Release()" has been called
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
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
          std::unique_ptr<StorageBackend::IAccessor> accessor(backend_->CreateAccessor());
          accessor->ReadWhole(visitor, uuid, type);
        }

        visitor.Release();

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
        std::unique_ptr<StorageBackend::IAccessor> accessor(backend_->CreateAccessor());
        accessor->Remove(uuid, type);
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
        OrthancPluginStorageReadRange readRange = NULL;
        if (backend_->HasReadRange())
        {
          readRange = StorageReadRange;
        }
        
        OrthancPluginRegisterStorageArea2(context_, StorageCreate, StorageReadWhole, readRange, StorageRemove);
        hasLoadedV2 = true;
      }
#  endif
#endif

      if (!hasLoadedV2)
      {
        LOG(WARNING) << "Performance warning: Your version of the Orthanc core doesn't support reading of file ranges";
        OrthancPluginRegisterStorageArea(context_, StorageCreate, StorageRead, StorageRemove);
      }
    }
  }


  void StorageBackend::Finalize()
  {
    backend_.reset(NULL);
    context_ = NULL;
  }


  class StorageBackend::StringVisitor : public StorageBackend::IFileContentVisitor
  {
  private:
    std::string&  target_;
    bool          success_;
      
  public:
    explicit StringVisitor(std::string& target) :
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
    

  void StorageBackend::ReadWholeToString(std::string& target,
                                         IAccessor& accessor,
                                         const std::string& uuid,
                                         OrthancPluginContentType type)
  {
    StringVisitor visitor(target);
    accessor.ReadWhole(visitor, uuid, type);

    if (!visitor.IsSuccess())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }
    

  void StorageBackend::ReadRangeToString(std::string& target,
                                         IAccessor& accessor,
                                         const std::string& uuid,
                                         OrthancPluginContentType type,
                                         uint64_t start,
                                         size_t length)
  {
    StringVisitor visitor(target);
    accessor.ReadRange(visitor, uuid, type, start, length);

    if (!visitor.IsSuccess())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }
}
