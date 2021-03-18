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



/**
 * NOTE: Until Orthanc 1.4.0, this file was part of the Orthanc source
 * distribution. This file is now part of "orthanc-databases", in
 * order to uncouple its evolution from the Orthanc core.
 **/

#pragma once

#include "IDatabaseBackend.h"

#include <OrthancException.h>



#define ORTHANC_PLUGINS_DATABASE_CATCH                                  \
  catch (::Orthanc::OrthancException& e)                                \
  {                                                                     \
    return static_cast<OrthancPluginErrorCode>(e.GetErrorCode());       \
  }                                                                     \
  catch (::std::runtime_error& e)                                       \
  {                                                                     \
    LogError(backend, e);                                               \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }                                                                     \
  catch (...)                                                           \
  {                                                                     \
    OrthancPluginLogError(backend->GetContext(), "Native exception");   \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }


#include <stdexcept>
#include <list>
#include <string>


namespace OrthancDatabases
{  
  /**
   * @brief Bridge between C and C++ database engines.
   * 
   * Class creating the bridge between the C low-level primitives for
   * custom database engines, and the high-level IDatabaseBackend C++
   * interface, for Orthanc <= 1.9.1.
   *
   * @ingroup Callbacks
   **/
  class DatabaseBackendAdapterV2
  {
  private:
    class Output : public IDatabaseBackendOutput
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
    

    // This class cannot be instantiated
    DatabaseBackendAdapterV2()
    {
    }

    static void LogError(IDatabaseBackend* backend,
                         const std::runtime_error& e)
    {
      const std::string message = "Exception in database back-end: " + std::string(e.what());
      OrthancPluginLogError(backend->GetContext(), message.c_str());
    }


    static OrthancPluginErrorCode  AddAttachment(void* payload,
                                                 int64_t id,
                                                 const OrthancPluginAttachment* attachment)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->AddAttachment(id, *attachment);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }

                             
    static OrthancPluginErrorCode  AttachChild(void* payload,
                                               int64_t parent,
                                               int64_t child)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->AttachChild(parent, child);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
                   
    static OrthancPluginErrorCode  ClearChanges(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->ClearChanges();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
                             

    static OrthancPluginErrorCode  ClearExportedResources(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->ClearExportedResources();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  CreateResource(int64_t* id, 
                                                  void* payload,
                                                  const char* publicId,
                                                  OrthancPluginResourceType resourceType)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *id = backend->CreateResource(publicId, resourceType);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  DeleteAttachment(void* payload,
                                                    int64_t id,
                                                    int32_t contentType)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        backend->DeleteAttachment(*output, id, contentType);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
   

    static OrthancPluginErrorCode  DeleteMetadata(void* payload,
                                                  int64_t id,
                                                  int32_t metadataType)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->DeleteMetadata(id, metadataType);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
   

    static OrthancPluginErrorCode  DeleteResource(void* payload,
                                                  int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        backend->DeleteResource(*output, id);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  GetAllInternalIds(OrthancPluginDatabaseContext* context,
                                                     void* payload,
                                                     OrthancPluginResourceType resourceType)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int64_t> target;
        backend->GetAllInternalIds(target, resourceType);

        for (std::list<int64_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<std::string> ids;
        backend->GetAllPublicIds(ids, resourceType);

        for (std::list<std::string>::const_iterator
               it = ids.begin(); it != ids.end(); ++it)
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<std::string> ids;
        backend->GetAllPublicIds(ids, resourceType, since, limit);

        for (std::list<std::string>::const_iterator
               it = ids.begin(); it != ids.end(); ++it)
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_Change);

      try
      {
        bool done;
        backend->GetChanges(*output, done, since, maxResult);
        
        if (done)
        {
          OrthancPluginDatabaseAnswerChangesDone(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int64_t> target;
        backend->GetChildrenInternalId(target, id);

        for (std::list<int64_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<std::string> ids;
        backend->GetChildrenPublicId(ids, id);

        for (std::list<std::string>::const_iterator
               it = ids.begin(); it != ids.end(); ++it)
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_ExportedResource);

      try
      {
        bool done;
        backend->GetExportedResources(*output, done, since, maxResult);

        if (done)
        {
          OrthancPluginDatabaseAnswerExportedResourcesDone(backend->GetContext(),
                                                           output->GetDatabase());
        }
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  GetLastChange(OrthancPluginDatabaseContext* context,
                                                 void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_Change);

      try
      {
        backend->GetLastChange(*output);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  GetLastExportedResource(OrthancPluginDatabaseContext* context,
                                                           void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_ExportedResource);

      try
      {
        backend->GetLastExportedResource(*output);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
    
               
    static OrthancPluginErrorCode  GetMainDicomTags(OrthancPluginDatabaseContext* context,
                                                    void* payload,
                                                    int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_DicomTag);

      try
      {
        backend->GetMainDicomTags(*output, id);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  GetPublicId(OrthancPluginDatabaseContext* context,
                                               void* payload,
                                               int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::string s = backend->GetPublicId(id);
        OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *target = backend->GetResourceCount(resourceType);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
                   

    static OrthancPluginErrorCode  GetResourceType(OrthancPluginResourceType* resourceType,
                                                   void* payload,
                                                   int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *resourceType = backend->GetResourceType(id);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  GetTotalCompressedSize(uint64_t* target,
                                                          void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *target = backend->GetTotalCompressedSize();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  GetTotalUncompressedSize(uint64_t* target,
                                                            void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *target = backend->GetTotalUncompressedSize();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
                   

    static OrthancPluginErrorCode  IsExistingResource(int32_t* existing,
                                                      void* payload,
                                                      int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *existing = backend->IsExistingResource(id);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  IsProtectedPatient(int32_t* isProtected,
                                                      void* payload,
                                                      int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *isProtected = backend->IsProtectedPatient(id);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  ListAvailableMetadata(OrthancPluginDatabaseContext* context,
                                                         void* payload,
                                                         int64_t id)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int32_t> target;
        backend->ListAvailableMetadata(target, id);

        for (std::list<int32_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt32(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int32_t> target;
        backend->ListAvailableAttachments(target, id);

        for (std::list<int32_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt32(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->LogChange(*change);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  LogExportedResource(void* payload,
                                                       const OrthancPluginExportedResource* exported)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->LogExportedResource(*exported);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }
          
         
    static OrthancPluginErrorCode  LookupAttachment(OrthancPluginDatabaseContext* context,
                                                    void* payload,
                                                    int64_t id,
                                                    int32_t contentType)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_Attachment);

      try
      {
        backend->LookupAttachment(*output, id, contentType);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  LookupGlobalProperty(OrthancPluginDatabaseContext* context,
                                                        void* payload,
                                                        int32_t property)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::string s;
        if (backend->LookupGlobalProperty(s, property))
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int64_t> target;
        backend->LookupIdentifier(target, resourceType, tag->group, tag->element, constraint, tag->value);

        for (std::list<int64_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<int64_t> target;
        backend->LookupIdentifierRange(target, resourceType, group, element, start, end);

        for (std::list<int64_t>::const_iterator
               it = target.begin(); it != target.end(); ++it)
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::string s;
        if (backend->LookupMetadata(s, id, metadata))
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        int64_t parent;
        if (backend->LookupParent(parent, id))
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        int64_t id;
        OrthancPluginResourceType type;
        if (backend->LookupResource(id, type, publicId))
        {
          OrthancPluginDatabaseAnswerResource(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        int64_t id;
        if (backend->SelectPatientToRecycle(id))
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        int64_t id;
        if (backend->SelectPatientToRecycle(id, patientIdToAvoid))
        {
          OrthancPluginDatabaseAnswerInt64(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetGlobalProperty(property, value);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  SetMainDicomTag(void* payload,
                                                   int64_t id,
                                                   const OrthancPluginDicomTag* tag)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetMainDicomTag(id, tag->group, tag->element, tag->value);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  SetIdentifierTag(void* payload,
                                                    int64_t id,
                                                    const OrthancPluginDicomTag* tag)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetIdentifierTag(id, tag->group, tag->element, tag->value);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  SetMetadata(void* payload,
                                               int64_t id,
                                               int32_t metadata,
                                               const char* value)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetMetadata(id, metadata, value);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode  SetProtectedPatient(void* payload,
                                                       int64_t id,
                                                       int32_t isProtected)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetProtectedPatient(id, (isProtected != 0));
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode StartTransaction(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->StartTransaction();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode RollbackTransaction(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->RollbackTransaction();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode CommitTransaction(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->CommitTransaction();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode Open(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->Open();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode Close(void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->Close();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode GetDatabaseVersion(uint32_t* version,
                                                     void* payload)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      
      try
      {
        *version = backend->GetDatabaseVersion();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }


    static OrthancPluginErrorCode UpgradeDatabase(void* payload,
                                                  uint32_t  targetVersion,
                                                  OrthancPluginStorageArea* storageArea)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      
      try
      {
        backend->UpgradeDatabase(targetVersion, storageArea);
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;
    }

    
    static OrthancPluginErrorCode ClearMainDicomTags(void* payload,
                                                     int64_t internalId)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      
      try
      {
        backend->ClearMainDicomTags(internalId);
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_MatchingResource);

      try
      {
        std::vector<Orthanc::DatabaseConstraint> lookup;
        lookup.reserve(constraintsCount);

        for (uint32_t i = 0; i < constraintsCount; i++)
        {
          lookup.push_back(Orthanc::DatabaseConstraint(constraints[i]));
        }
        
        backend->LookupResources(*output, lookup, queryLevel, limit, (requestSomeInstance != 0));
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->CreateInstance(*target, hashPatient, hashStudy, hashSeries, hashInstance);
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->SetResourcesContent(countIdentifierTags, identifierTags,
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_None);

      try
      {
        std::list<std::string> values;
        backend->GetChildrenMetadata(values, resourceId, metadata);

        for (std::list<std::string>::const_iterator
               it = values.begin(); it != values.end(); ++it)
        {
          OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        *result = backend->GetLastChangeIndex();
        return OrthancPluginErrorCode_Success;
      }
      ORTHANC_PLUGINS_DATABASE_CATCH;      
    }


    // New primitive since Orthanc 1.5.2
    static OrthancPluginErrorCode TagMostRecentPatient(void* payload,
                                                       int64_t patientId)
    {
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);

      try
      {
        backend->TagMostRecentPatient(patientId);
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_Metadata);

      try
      {
        std::map<int32_t, std::string> result;
        backend->GetAllMetadata(result, resourceId);

        for (std::map<int32_t, std::string>::const_iterator
               it = result.begin(); it != result.end(); ++it)
        {
          OrthancPluginDatabaseAnswerMetadata(backend->GetContext(),
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
      IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(payload);
      std::unique_ptr<Output> output(dynamic_cast<Output*>(backend->CreateOutput()));
      output->SetAllowedAnswers(Output::AllowedAnswers_String);

      try
      {
        std::string parent;
        if (backend->LookupResourceAndParent(*id, *type, parent, publicId))
        {
          *isExisting = 1;

          if (!parent.empty())
          {
            OrthancPluginDatabaseAnswerString(backend->GetContext(),
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
   

  public:
    class Factory : public IDatabaseBackendOutput::IFactory
    {
    private:
      OrthancPluginContext*         context_;
      OrthancPluginDatabaseContext* database_;

    public:
      Factory(OrthancPluginContext*         context,
              OrthancPluginDatabaseContext* database) :
        context_(context),
        database_(database)
      {
      }

      virtual IDatabaseBackendOutput* CreateOutput() ORTHANC_OVERRIDE
      {
        return new Output(context_, database_);
      }
    };


    /**
     * Register a custom database back-end written in C++.
     *
     * @param context The Orthanc plugin context, as received by OrthancPluginInitialize().
     * @param backend Your custom database engine.
     **/

    static void Register(OrthancPluginContext* context,
                         IDatabaseBackend& backend)
    {
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

      if (backend.HasCreateInstance())
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
        OrthancPluginRegisterDatabaseBackendV2(context, &params, &extensions, &backend);
      if (!context)
      {
        throw std::runtime_error("Unable to register the database backend");
      }

      backend.SetOutputFactory(new Factory(context, database));
    }
  };
}
