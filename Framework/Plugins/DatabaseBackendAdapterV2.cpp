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



#include "DatabaseBackendAdapterV2.h"
#include "GlobalProperties.h"

#include <OrthancException.h>

#include <boost/thread/mutex.hpp>
#include <list>
#include <stdexcept>
#include <string>


#define ORTHANC_PLUGINS_DATABASE_CATCH                                  \
  catch (::Orthanc::OrthancException& e)                                \
  {                                                                     \
    return static_cast<OrthancPluginErrorCode>(e.GetErrorCode());       \
  }                                                                     \
  catch (::std::runtime_error& e)                                       \
  {                                                                     \
    LogError(adapter->GetBackend(), e);                                 \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }                                                                     \
  catch (...)                                                           \
  {                                                                     \
    OrthancPluginLogError(adapter->GetBackend().GetContext(), "Native exception"); \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }


namespace OrthancDatabases
{
  class DatabaseBackendAdapterV2::Adapter : public boost::noncopyable
  {
  private:
    std::unique_ptr<IDatabaseBackend>  backend_;
    boost::mutex                       databaseMutex_;
    std::unique_ptr<DatabaseManager>   database_;

  public:
    Adapter(IDatabaseBackend* backend) :
      backend_(backend)
    {
      if (backend == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
      }
    }

    IDatabaseBackend& GetBackend() const
    {
      return *backend_;
    }

    void OpenConnection()
    {
      boost::mutex::scoped_lock  lock(databaseMutex_);

      if (database_.get() == NULL)
      {
        database_.reset(new DatabaseManager(backend_->CreateDatabaseFactory()));
        database_->Open();
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
    }

    void CloseConnection()
    {
      boost::mutex::scoped_lock  lock(databaseMutex_);

      if (database_.get() == NULL)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      else
      {
        database_->Close();
        database_.reset(NULL);
      }
    }

    class DatabaseAccessor : public boost::noncopyable
    {
    private:
      boost::mutex::scoped_lock  lock_;
      DatabaseManager*           database_;
      
    public:
      DatabaseAccessor(Adapter& adapter) :
        lock_(adapter.databaseMutex_),
        database_(adapter.database_.get())
      {
        if (database_ == NULL)
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }
      }

      DatabaseManager& GetDatabase() const
      {
        assert(database_ != NULL);
        return *database_;
      }
    };
  };

  
  class DatabaseBackendAdapterV2::Output : public IDatabaseBackendOutput
  {
  public:
    enum AllowedAnswers
    {
      AllowedAnswers_All,
      AllowedAnswers_None,
      AllowedAnswers_Attachment,
      AllowedAnswers_Change,
      AllowedAnswers_DicomTag,
      AllowedAnswers_ExportedResource,
      AllowedAnswers_MatchingResource,
      AllowedAnswers_String,
      AllowedAnswers_Metadata
    };

  private:
    OrthancPluginContext*         context_;
    OrthancPluginDatabaseContext* database_;
    AllowedAnswers                allowedAnswers_;

  public:   
    Output(OrthancPluginContext*         context,
           OrthancPluginDatabaseContext* database) :
      context_(context),
      database_(database),
      allowedAnswers_(AllowedAnswers_All /* for unit tests */)
    {
    }

    void SetAllowedAnswers(AllowedAnswers allowed)
    {
      allowedAnswers_ = allowed;
    }

    OrthancPluginDatabaseContext* GetDatabase() const
    {
      return database_;
    }

    virtual void SignalDeletedAttachment(const std::string& uuid,
                                         int32_t            contentType,
                                         uint64_t           uncompressedSize,
                                         const std::string& uncompressedHash,
                                         int32_t            compressionType,
                                         uint64_t           compressedSize,
                                         const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      OrthancPluginAttachment attachment;
      attachment.uuid = uuid.c_str();
      attachment.contentType = contentType;
      attachment.uncompressedSize = uncompressedSize;
      attachment.uncompressedHash = uncompressedHash.c_str();
      attachment.compressionType = compressionType;
      attachment.compressedSize = compressedSize;
      attachment.compressedHash = compressedHash.c_str();

      OrthancPluginDatabaseSignalDeletedAttachment(context_, database_, &attachment);
    }

    virtual void SignalDeletedResource(const std::string& publicId,
                                       OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE
    {
      OrthancPluginDatabaseSignalDeletedResource(context_, database_, publicId.c_str(), resourceType);
    }

    virtual void SignalRemainingAncestor(const std::string& ancestorId,
                                         OrthancPluginResourceType ancestorType) ORTHANC_OVERRIDE
    {
      OrthancPluginDatabaseSignalRemainingAncestor(context_, database_, ancestorId.c_str(), ancestorType);
    }

    virtual void AnswerAttachment(const std::string& uuid,
                                  int32_t            contentType,
                                  uint64_t           uncompressedSize,
                                  const std::string& uncompressedHash,
                                  int32_t            compressionType,
                                  uint64_t           compressedSize,
                                  const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_Attachment)
      {
        throw std::runtime_error("Cannot answer with an attachment in the current state");
      }

      OrthancPluginAttachment attachment;
      attachment.uuid = uuid.c_str();
      attachment.contentType = contentType;
      attachment.uncompressedSize = uncompressedSize;
      attachment.uncompressedHash = uncompressedHash.c_str();
      attachment.compressionType = compressionType;
      attachment.compressedSize = compressedSize;
      attachment.compressedHash = compressedHash.c_str();

      OrthancPluginDatabaseAnswerAttachment(context_, database_, &attachment);
    }

    virtual void AnswerChange(int64_t                    seq,
                              int32_t                    changeType,
                              OrthancPluginResourceType  resourceType,
                              const std::string&         publicId,
                              const std::string&         date) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_Change)
      {
        throw std::runtime_error("Cannot answer with a change in the current state");
      }

      OrthancPluginChange change;
      change.seq = seq;
      change.changeType = changeType;
      change.resourceType = resourceType;
      change.publicId = publicId.c_str();
      change.date = date.c_str();

      OrthancPluginDatabaseAnswerChange(context_, database_, &change);
    }

    virtual void AnswerDicomTag(uint16_t group,
                                uint16_t element,
                                const std::string& value) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_DicomTag)
      {
        throw std::runtime_error("Cannot answer with a DICOM tag in the current state");
      }

      OrthancPluginDicomTag tag;
      tag.group = group;
      tag.element = element;
      tag.value = value.c_str();

      OrthancPluginDatabaseAnswerDicomTag(context_, database_, &tag);
    }

    virtual void AnswerExportedResource(int64_t                    seq,
                                        OrthancPluginResourceType  resourceType,
                                        const std::string&         publicId,
                                        const std::string&         modality,
                                        const std::string&         date,
                                        const std::string&         patientId,
                                        const std::string&         studyInstanceUid,
                                        const std::string&         seriesInstanceUid,
                                        const std::string&         sopInstanceUid) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_ExportedResource)
      {
        throw std::runtime_error("Cannot answer with an exported resource in the current state");
      }

      OrthancPluginExportedResource exported;
      exported.seq = seq;
      exported.resourceType = resourceType;
      exported.publicId = publicId.c_str();
      exported.modality = modality.c_str();
      exported.date = date.c_str();
      exported.patientId = patientId.c_str();
      exported.studyInstanceUid = studyInstanceUid.c_str();
      exported.seriesInstanceUid = seriesInstanceUid.c_str();
      exported.sopInstanceUid = sopInstanceUid.c_str();

      OrthancPluginDatabaseAnswerExportedResource(context_, database_, &exported);
    }


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void AnswerMatchingResource(const std::string& resourceId) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_MatchingResource)
      {
        throw std::runtime_error("Cannot answer with an exported resource in the current state");
      }

      OrthancPluginMatchingResource match;
      match.resourceId = resourceId.c_str();
      match.someInstanceId = NULL;

      OrthancPluginDatabaseAnswerMatchingResource(context_, database_, &match);
    }
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void AnswerMatchingResource(const std::string& resourceId,
                                        const std::string& someInstanceId) ORTHANC_OVERRIDE
    {
      if (allowedAnswers_ != AllowedAnswers_All &&
          allowedAnswers_ != AllowedAnswers_MatchingResource)
      {
        throw std::runtime_error("Cannot answer with an exported resource in the current state");
      }

      OrthancPluginMatchingResource match;
      match.resourceId = resourceId.c_str();
      match.someInstanceId = someInstanceId.c_str();

      OrthancPluginDatabaseAnswerMatchingResource(context_, database_, &match);
    }
#endif
  };
    

  static void LogError(IDatabaseBackend& backend,
                       const std::runtime_error& e)
  {
    const std::string message = "Exception in database back-end: " + std::string(e.what());
    OrthancPluginLogError(backend.GetContext(), message.c_str());
  }


  static OrthancPluginErrorCode  AddAttachment(void* payload,
                                               int64_t id,
                                               const OrthancPluginAttachment* attachment)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      //DatabaseBackendAdapterV2::Adapter::DatabaseAccessor accessor(*adapter);    // TODO
      
      adapter->GetBackend().AddAttachment(id, *attachment);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }

                             
  static OrthancPluginErrorCode  AttachChild(void* payload,
                                             int64_t parent,
                                             int64_t child)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().AttachChild(parent, child);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
                   
  static OrthancPluginErrorCode  ClearChanges(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().ClearChanges();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
                             

  static OrthancPluginErrorCode  ClearExportedResources(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().ClearExportedResources();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  CreateResource(int64_t* id, 
                                                void* payload,
                                                const char* publicId,
                                                OrthancPluginResourceType resourceType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *id = adapter->GetBackend().CreateResource(publicId, resourceType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  DeleteAttachment(void* payload,
                                                  int64_t id,
                                                  int32_t contentType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      adapter->GetBackend().DeleteAttachment(*output, id, contentType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
   

  static OrthancPluginErrorCode  DeleteMetadata(void* payload,
                                                int64_t id,
                                                int32_t metadataType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().DeleteMetadata(id, metadataType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
   

  static OrthancPluginErrorCode  DeleteResource(void* payload,
                                                int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      adapter->GetBackend().DeleteResource(*output, id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetAllInternalIds(OrthancPluginDatabaseContext* context,
                                                   void* payload,
                                                   OrthancPluginResourceType resourceType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int64_t> target;
      adapter->GetBackend().GetAllInternalIds(target, resourceType);

      for (std::list<int64_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetAllPublicIds(OrthancPluginDatabaseContext* context,
                                                 void* payload,
                                                 OrthancPluginResourceType resourceType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<std::string> ids;
      adapter->GetBackend().GetAllPublicIds(ids, resourceType);

      for (std::list<std::string>::const_iterator
             it = ids.begin(); it != ids.end(); ++it)
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(),
                                          it->c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetAllPublicIdsWithLimit(OrthancPluginDatabaseContext* context,
                                                          void* payload,
                                                          OrthancPluginResourceType resourceType,
                                                          uint64_t since,
                                                          uint64_t limit)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<std::string> ids;
      adapter->GetBackend().GetAllPublicIds(ids, resourceType, since, limit);

      for (std::list<std::string>::const_iterator
             it = ids.begin(); it != ids.end(); ++it)
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(),
                                          it->c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetChanges(OrthancPluginDatabaseContext* context,
                                            void* payload,
                                            int64_t since,
                                            uint32_t maxResult)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_Change);

    try
    {
      bool done;
      adapter->GetBackend().GetChanges(*output, done, since, maxResult);
        
      if (done)
      {
        OrthancPluginDatabaseAnswerChangesDone(adapter->GetBackend().GetContext(),
                                               output->GetDatabase());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetChildrenInternalId(OrthancPluginDatabaseContext* context,
                                                       void* payload,
                                                       int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int64_t> target;
      adapter->GetBackend().GetChildrenInternalId(target, id);

      for (std::list<int64_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  GetChildrenPublicId(OrthancPluginDatabaseContext* context,
                                                     void* payload,
                                                     int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<std::string> ids;
      adapter->GetBackend().GetChildrenPublicId(ids, id);

      for (std::list<std::string>::const_iterator
             it = ids.begin(); it != ids.end(); ++it)
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(),
                                          it->c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetExportedResources(OrthancPluginDatabaseContext* context,
                                                      void* payload,
                                                      int64_t  since,
                                                      uint32_t  maxResult)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_ExportedResource);

    try
    {
      bool done;
      adapter->GetBackend().GetExportedResources(*output, done, since, maxResult);

      if (done)
      {
        OrthancPluginDatabaseAnswerExportedResourcesDone(adapter->GetBackend().GetContext(),
                                                         output->GetDatabase());
      }
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  GetLastChange(OrthancPluginDatabaseContext* context,
                                               void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_Change);

    try
    {
      adapter->GetBackend().GetLastChange(*output);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetLastExportedResource(OrthancPluginDatabaseContext* context,
                                                         void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_ExportedResource);

    try
    {
      adapter->GetBackend().GetLastExportedResource(*output);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
    
               
  static OrthancPluginErrorCode  GetMainDicomTags(OrthancPluginDatabaseContext* context,
                                                  void* payload,
                                                  int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_DicomTag);

    try
    {
      adapter->GetBackend().GetMainDicomTags(*output, id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  GetPublicId(OrthancPluginDatabaseContext* context,
                                             void* payload,
                                             int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::string s = adapter->GetBackend().GetPublicId(id);
      OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                        output->GetDatabase(),
                                        s.c_str());

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetResourceCount(uint64_t* target,
                                                  void* payload,
                                                  OrthancPluginResourceType  resourceType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *target = adapter->GetBackend().GetResourcesCount(resourceType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
                   

  static OrthancPluginErrorCode  GetResourceType(OrthancPluginResourceType* resourceType,
                                                 void* payload,
                                                 int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *resourceType = adapter->GetBackend().GetResourceType(id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  GetTotalCompressedSize(uint64_t* target,
                                                        void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *target = adapter->GetBackend().GetTotalCompressedSize();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  GetTotalUncompressedSize(uint64_t* target,
                                                          void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *target = adapter->GetBackend().GetTotalUncompressedSize();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
                   

  static OrthancPluginErrorCode  IsExistingResource(int32_t* existing,
                                                    void* payload,
                                                    int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *existing = adapter->GetBackend().IsExistingResource(id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  IsProtectedPatient(int32_t* isProtected,
                                                    void* payload,
                                                    int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *isProtected = adapter->GetBackend().IsProtectedPatient(id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  ListAvailableMetadata(OrthancPluginDatabaseContext* context,
                                                       void* payload,
                                                       int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int32_t> target;
      adapter->GetBackend().ListAvailableMetadata(target, id);

      for (std::list<int32_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt32(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(),
                                         *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  ListAvailableAttachments(OrthancPluginDatabaseContext* context,
                                                          void* payload,
                                                          int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int32_t> target;
      adapter->GetBackend().ListAvailableAttachments(target, id);

      for (std::list<int32_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt32(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(),
                                         *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LogChange(void* payload,
                                           const OrthancPluginChange* change)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      int64_t id;
      OrthancPluginResourceType type;
      if (!adapter->GetBackend().LookupResource(id, type, change->publicId) ||
          type != change->resourceType)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
      }
      else
      {
        adapter->GetBackend().LogChange(change->changeType, id, type, change->date);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  LogExportedResource(void* payload,
                                                     const OrthancPluginExportedResource* exported)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().LogExportedResource(*exported);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
          
         
  static OrthancPluginErrorCode  LookupAttachment(OrthancPluginDatabaseContext* context,
                                                  void* payload,
                                                  int64_t id,
                                                  int32_t contentType)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_Attachment);

    try
    {
      adapter->GetBackend().LookupAttachment(*output, id, contentType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupGlobalProperty(OrthancPluginDatabaseContext* context,
                                                      void* payload,
                                                      int32_t property)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::string s;
      if (adapter->GetBackend().LookupGlobalProperty(s, MISSING_SERVER_IDENTIFIER, property))
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(),
                                          s.c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupIdentifier3(OrthancPluginDatabaseContext* context,
                                                   void* payload,
                                                   OrthancPluginResourceType resourceType,
                                                   const OrthancPluginDicomTag* tag,
                                                   OrthancPluginIdentifierConstraint constraint)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int64_t> target;
      adapter->GetBackend().LookupIdentifier(target, resourceType, tag->group, tag->element, constraint, tag->value);

      for (std::list<int64_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupIdentifierRange(OrthancPluginDatabaseContext* context,
                                                       void* payload,
                                                       OrthancPluginResourceType resourceType,
                                                       uint16_t group,
                                                       uint16_t element,
                                                       const char* start,
                                                       const char* end)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<int64_t> target;
      adapter->GetBackend().LookupIdentifierRange(target, resourceType, group, element, start, end);

      for (std::list<int64_t>::const_iterator
             it = target.begin(); it != target.end(); ++it)
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), *it);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupMetadata(OrthancPluginDatabaseContext* context,
                                                void* payload,
                                                int64_t id,
                                                int32_t metadata)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::string s;
      if (adapter->GetBackend().LookupMetadata(s, id, metadata))
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(), s.c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupParent(OrthancPluginDatabaseContext* context,
                                              void* payload,
                                              int64_t id)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      int64_t parent;
      if (adapter->GetBackend().LookupParent(parent, id))
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), parent);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  LookupResource(OrthancPluginDatabaseContext* context,
                                                void* payload,
                                                const char* publicId)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      int64_t id;
      OrthancPluginResourceType type;
      if (adapter->GetBackend().LookupResource(id, type, publicId))
      {
        OrthancPluginDatabaseAnswerResource(adapter->GetBackend().GetContext(),
                                            output->GetDatabase(), 
                                            id, type);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SelectPatientToRecycle(OrthancPluginDatabaseContext* context,
                                                        void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      int64_t id;
      if (adapter->GetBackend().SelectPatientToRecycle(id))
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), id);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SelectPatientToRecycle2(OrthancPluginDatabaseContext* context,
                                                         void* payload,
                                                         int64_t patientIdToAvoid)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      int64_t id;
      if (adapter->GetBackend().SelectPatientToRecycle(id, patientIdToAvoid))
      {
        OrthancPluginDatabaseAnswerInt64(adapter->GetBackend().GetContext(),
                                         output->GetDatabase(), id);
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SetGlobalProperty(void* payload,
                                                   int32_t property,
                                                   const char* value)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetGlobalProperty(MISSING_SERVER_IDENTIFIER, property, value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SetMainDicomTag(void* payload,
                                                 int64_t id,
                                                 const OrthancPluginDicomTag* tag)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetMainDicomTag(id, tag->group, tag->element, tag->value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SetIdentifierTag(void* payload,
                                                  int64_t id,
                                                  const OrthancPluginDicomTag* tag)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetIdentifierTag(id, tag->group, tag->element, tag->value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SetMetadata(void* payload,
                                             int64_t id,
                                             int32_t metadata,
                                             const char* value)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetMetadata(id, metadata, value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode  SetProtectedPatient(void* payload,
                                                     int64_t id,
                                                     int32_t isProtected)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetProtectedPatient(id, (isProtected != 0));
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode StartTransaction(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().StartTransaction(TransactionType_ReadWrite);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode RollbackTransaction(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().RollbackTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode CommitTransaction(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().CommitTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode Open(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      //adapter->OpenConnection();  // TODO
      adapter->GetBackend().Open();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode Close(void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().Close();
      //adapter->CloseConnection();  // TODO
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode GetDatabaseVersion(uint32_t* version,
                                                   void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
      
    try
    {
      *version = adapter->GetBackend().GetDatabaseVersion();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


  static OrthancPluginErrorCode UpgradeDatabase(void* payload,
                                                uint32_t  targetVersion,
                                                OrthancPluginStorageArea* storageArea)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
      
    try
    {
      adapter->GetBackend().UpgradeDatabase(targetVersion, storageArea);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }

    
  static OrthancPluginErrorCode ClearMainDicomTags(void* payload,
                                                   int64_t internalId)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
      
    try
    {
      adapter->GetBackend().ClearMainDicomTags(internalId);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  /* Use GetOutput().AnswerResource() */
  static OrthancPluginErrorCode LookupResources(
    OrthancPluginDatabaseContext* context,
    void* payload,
    uint32_t constraintsCount,
    const OrthancPluginDatabaseConstraint* constraints,
    OrthancPluginResourceType queryLevel,
    uint32_t limit,
    uint8_t requestSomeInstance)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_MatchingResource);

    try
    {
      std::vector<Orthanc::DatabaseConstraint> lookup;
      lookup.reserve(constraintsCount);

      for (uint32_t i = 0; i < constraintsCount; i++)
      {
        lookup.push_back(Orthanc::DatabaseConstraint(constraints[i]));
      }
        
      adapter->GetBackend().LookupResources(*output, lookup, queryLevel, limit, (requestSomeInstance != 0));
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;
  }
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static OrthancPluginErrorCode CreateInstance(OrthancPluginCreateInstanceResult* target,
                                               void* payload,
                                               const char* hashPatient,
                                               const char* hashStudy,
                                               const char* hashSeries,
                                               const char* hashInstance)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().CreateInstance(*target, hashPatient, hashStudy, hashSeries, hashInstance);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static OrthancPluginErrorCode SetResourcesContent(
    void* payload,
    uint32_t countIdentifierTags,
    const OrthancPluginResourcesContentTags* identifierTags,
    uint32_t countMainDicomTags,
    const OrthancPluginResourcesContentTags* mainDicomTags,
    uint32_t countMetadata,
    const OrthancPluginResourcesContentMetadata* metadata)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().SetResourcesContent(countIdentifierTags, identifierTags,
                                   countMainDicomTags, mainDicomTags,
                                   countMetadata, metadata);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }
#endif    

    
  // New primitive since Orthanc 1.5.2
  static OrthancPluginErrorCode GetChildrenMetadata(OrthancPluginDatabaseContext* context,
                                                    void* payload,
                                                    int64_t resourceId,
                                                    int32_t metadata)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_None);

    try
    {
      std::list<std::string> values;
      adapter->GetBackend().GetChildrenMetadata(values, resourceId, metadata);

      for (std::list<std::string>::const_iterator
             it = values.begin(); it != values.end(); ++it)
      {
        OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                          output->GetDatabase(),
                                          it->c_str());
      }

      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }


  // New primitive since Orthanc 1.5.2
  static OrthancPluginErrorCode GetLastChangeIndex(int64_t* result,
                                                   void* payload)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      *result = adapter->GetBackend().GetLastChangeIndex();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }


  // New primitive since Orthanc 1.5.2
  static OrthancPluginErrorCode TagMostRecentPatient(void* payload,
                                                     int64_t patientId)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);

    try
    {
      adapter->GetBackend().TagMostRecentPatient(patientId);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }
   

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
  // New primitive since Orthanc 1.5.4
  static OrthancPluginErrorCode GetAllMetadata(OrthancPluginDatabaseContext* context,
                                               void* payload,
                                               int64_t resourceId)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_Metadata);

    try
    {
      std::map<int32_t, std::string> result;
      adapter->GetBackend().GetAllMetadata(result, resourceId);

      for (std::map<int32_t, std::string>::const_iterator
             it = result.begin(); it != result.end(); ++it)
      {
        OrthancPluginDatabaseAnswerMetadata(adapter->GetBackend().GetContext(),
                                            output->GetDatabase(),
                                            resourceId, it->first, it->second.c_str());
      }
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }
#  endif
#endif
   

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
  // New primitive since Orthanc 1.5.4
  static OrthancPluginErrorCode LookupResourceAndParent(OrthancPluginDatabaseContext* context,
                                                        uint8_t* isExisting,
                                                        int64_t* id,
                                                        OrthancPluginResourceType* type,
                                                        void* payload,
                                                        const char* publicId)
  {
    DatabaseBackendAdapterV2::Adapter* adapter = reinterpret_cast<DatabaseBackendAdapterV2::Adapter*>(payload);
    std::unique_ptr<DatabaseBackendAdapterV2::Output> output(dynamic_cast<DatabaseBackendAdapterV2::Output*>(adapter->GetBackend().CreateOutput()));
    output->SetAllowedAnswers(DatabaseBackendAdapterV2::Output::AllowedAnswers_String);

    try
    {
      std::string parent;
      if (adapter->GetBackend().LookupResourceAndParent(*id, *type, parent, publicId))
      {
        *isExisting = 1;

        if (!parent.empty())
        {
          OrthancPluginDatabaseAnswerString(adapter->GetBackend().GetContext(),
                                            output->GetDatabase(),
                                            parent.c_str());
        }
      }
      else
      {
        *isExisting = 0;
      }
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH;      
  }
#  endif
#endif
   

  IDatabaseBackendOutput* DatabaseBackendAdapterV2::Factory::CreateOutput()
  {
    return new Output(context_, database_);
  }


  static std::unique_ptr<DatabaseBackendAdapterV2::Adapter> adapter_;

  void DatabaseBackendAdapterV2::Register(IDatabaseBackend* backend)
  {
    if (backend == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }

    if (adapter_.get() != NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    adapter_.reset(new Adapter(backend));
    
    OrthancPluginDatabaseBackend  params;
    memset(&params, 0, sizeof(params));

    OrthancPluginDatabaseExtensions  extensions;
    memset(&extensions, 0, sizeof(extensions));

    params.addAttachment = AddAttachment;
    params.attachChild = AttachChild;
    params.clearChanges = ClearChanges;
    params.clearExportedResources = ClearExportedResources;
    params.createResource = CreateResource;
    params.deleteAttachment = DeleteAttachment;
    params.deleteMetadata = DeleteMetadata;
    params.deleteResource = DeleteResource;
    params.getAllPublicIds = GetAllPublicIds;
    params.getChanges = GetChanges;
    params.getChildrenInternalId = GetChildrenInternalId;
    params.getChildrenPublicId = GetChildrenPublicId;
    params.getExportedResources = GetExportedResources;
    params.getLastChange = GetLastChange;
    params.getLastExportedResource = GetLastExportedResource;
    params.getMainDicomTags = GetMainDicomTags;
    params.getPublicId = GetPublicId;
    params.getResourceCount = GetResourceCount;
    params.getResourceType = GetResourceType;
    params.getTotalCompressedSize = GetTotalCompressedSize;
    params.getTotalUncompressedSize = GetTotalUncompressedSize;
    params.isExistingResource = IsExistingResource;
    params.isProtectedPatient = IsProtectedPatient;
    params.listAvailableMetadata = ListAvailableMetadata;
    params.listAvailableAttachments = ListAvailableAttachments;
    params.logChange = LogChange;
    params.logExportedResource = LogExportedResource;
    params.lookupAttachment = LookupAttachment;
    params.lookupGlobalProperty = LookupGlobalProperty;
    params.lookupIdentifier = NULL;    // Unused starting with Orthanc 0.9.5 (db v6)
    params.lookupIdentifier2 = NULL;   // Unused starting with Orthanc 0.9.5 (db v6)
    params.lookupMetadata = LookupMetadata;
    params.lookupParent = LookupParent;
    params.lookupResource = LookupResource;
    params.selectPatientToRecycle = SelectPatientToRecycle;
    params.selectPatientToRecycle2 = SelectPatientToRecycle2;
    params.setGlobalProperty = SetGlobalProperty;
    params.setMainDicomTag = SetMainDicomTag;
    params.setIdentifierTag = SetIdentifierTag;
    params.setMetadata = SetMetadata;
    params.setProtectedPatient = SetProtectedPatient;
    params.startTransaction = StartTransaction;
    params.rollbackTransaction = RollbackTransaction;
    params.commitTransaction = CommitTransaction;
    params.open = Open;
    params.close = Close;

    extensions.getAllPublicIdsWithLimit = GetAllPublicIdsWithLimit;
    extensions.getDatabaseVersion = GetDatabaseVersion;
    extensions.upgradeDatabase = UpgradeDatabase;
    extensions.clearMainDicomTags = ClearMainDicomTags;
    extensions.getAllInternalIds = GetAllInternalIds;     // New in Orthanc 0.9.5 (db v6)
    extensions.lookupIdentifier3 = LookupIdentifier3;     // New in Orthanc 0.9.5 (db v6)

    bool performanceWarning = true;

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 4, 0)
    extensions.lookupIdentifierRange = LookupIdentifierRange;    // New in Orthanc 1.4.0
#  endif
#endif

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    // Optimizations brought by Orthanc 1.5.2
    extensions.lookupResources = LookupResources;          // Fast lookup
    extensions.setResourcesContent = SetResourcesContent;  // Fast setting tags/metadata
    extensions.getChildrenMetadata = GetChildrenMetadata;
    extensions.getLastChangeIndex = GetLastChangeIndex;
    extensions.tagMostRecentPatient = TagMostRecentPatient;

    if (adapter_->GetBackend().HasCreateInstance())
    {
      extensions.createInstance = CreateInstance;          // Fast creation of resources
    }
#endif      

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
    // Optimizations brought by Orthanc 1.5.4
    extensions.lookupResourceAndParent = LookupResourceAndParent;
    extensions.getAllMetadata = GetAllMetadata;
    performanceWarning = false;
#  endif
#endif

    OrthancPluginContext* context = adapter_->GetBackend().GetContext();
    
    if (performanceWarning)
    {
      char info[1024];
      sprintf(info, 
              "Performance warning: The database index plugin was compiled "
              "against an old version of the Orthanc SDK (%d.%d.%d): "
              "Consider upgrading to version %d.%d.%d of the Orthanc SDK",
              ORTHANC_PLUGINS_MINIMAL_MAJOR_NUMBER,
              ORTHANC_PLUGINS_MINIMAL_MINOR_NUMBER,
              ORTHANC_PLUGINS_MINIMAL_REVISION_NUMBER,
              ORTHANC_OPTIMAL_VERSION_MAJOR,
              ORTHANC_OPTIMAL_VERSION_MINOR,
              ORTHANC_OPTIMAL_VERSION_REVISION);

      OrthancPluginLogWarning(context, info);
    }

    OrthancPluginDatabaseContext* database =
      OrthancPluginRegisterDatabaseBackendV2(context, &params, &extensions, adapter_.get());
    if (database == NULL)
    {
      throw std::runtime_error("Unable to register the database backend");
    }

    adapter_->GetBackend().SetOutputFactory(new Factory(context, database));
  }


  void DatabaseBackendAdapterV2::Finalize()
  {
    adapter_.reset(NULL);
  }
}
