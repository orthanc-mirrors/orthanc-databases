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


#include "DatabaseBackendAdapterV4.h"

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 12, 0)

#include "IndexConnectionsPool.h"

#include <OrthancDatabasePlugin.pb.h>  // Include protobuf messages

#include <Logging.h>
#include <OrthancException.h>

#include <stdexcept>
#include <list>
#include <string>
#include <cassert>


#define ORTHANC_PLUGINS_DATABASE_CATCH(context) \


namespace OrthancDatabases
{
  static bool isBackendInUse_ = false;  // Only for sanity checks


  static Orthanc::DatabasePluginMessages::ResourceType Convert(OrthancPluginResourceType resourceType)
  {
    switch (resourceType)
    {
      case OrthancPluginResourceType_Patient:
        return Orthanc::DatabasePluginMessages::RESOURCE_PATIENT;

      case OrthancPluginResourceType_Study:
        return Orthanc::DatabasePluginMessages::RESOURCE_STUDY;

      case OrthancPluginResourceType_Series:
        return Orthanc::DatabasePluginMessages::RESOURCE_SERIES;

      case OrthancPluginResourceType_Instance:
        return Orthanc::DatabasePluginMessages::RESOURCE_INSTANCE;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }


  static OrthancPluginResourceType Convert(Orthanc::DatabasePluginMessages::ResourceType resourceType)
  {
    switch (resourceType)
    {
      case Orthanc::DatabasePluginMessages::RESOURCE_PATIENT:
        return OrthancPluginResourceType_Patient;

      case Orthanc::DatabasePluginMessages::RESOURCE_STUDY:
        return OrthancPluginResourceType_Study;

      case Orthanc::DatabasePluginMessages::RESOURCE_SERIES:
        return OrthancPluginResourceType_Series;

      case Orthanc::DatabasePluginMessages::RESOURCE_INSTANCE:
        return OrthancPluginResourceType_Instance;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }


  class Output : public IDatabaseBackendOutput
  {
  private:
    Orthanc::DatabasePluginMessages::DeleteAttachment::Response*         deleteAttachment_;
    Orthanc::DatabasePluginMessages::DeleteResource::Response*           deleteResource_;
    Orthanc::DatabasePluginMessages::GetChanges::Response*               getChanges_;
    Orthanc::DatabasePluginMessages::GetExportedResources::Response*     getExportedResources_;
    Orthanc::DatabasePluginMessages::GetLastChange::Response*            getLastChange_;
    Orthanc::DatabasePluginMessages::GetLastExportedResource::Response*  getLastExportedResource_;
    Orthanc::DatabasePluginMessages::GetMainDicomTags::Response*         getMainDicomTags_;

    void Clear()
    {
      deleteAttachment_ = NULL;
      deleteResource_ = NULL;
      getChanges_ = NULL;
      getExportedResources_ = NULL;
      getLastChange_ = NULL;
      getLastExportedResource_ = NULL;
    }
    
  public:
    Output(Orthanc::DatabasePluginMessages::DeleteAttachment::Response& deleteAttachment)
    {
      Clear();
      deleteAttachment_ = &deleteAttachment;
    }
    
    Output(Orthanc::DatabasePluginMessages::DeleteResource::Response& deleteResource)
    {
      Clear();
      deleteResource_ = &deleteResource;
    }
    
    Output(Orthanc::DatabasePluginMessages::GetChanges::Response& getChanges)
    {
      Clear();
      getChanges_ = &getChanges;
    }
    
    Output(Orthanc::DatabasePluginMessages::GetExportedResources::Response& getExportedResources)
    {
      Clear();
      getExportedResources_ = &getExportedResources;
    }
    
    Output(Orthanc::DatabasePluginMessages::GetLastChange::Response& getLastChange)
    {
      Clear();
      getLastChange_ = &getLastChange;
    }
    
    Output(Orthanc::DatabasePluginMessages::GetLastExportedResource::Response& getLastExportedResource)
    {
      Clear();
      getLastExportedResource_ = &getLastExportedResource;
    }
    
    Output(Orthanc::DatabasePluginMessages::GetMainDicomTags::Response& getMainDicomTags)
    {
      Clear();
      getMainDicomTags_ = &getMainDicomTags;
    }
    
    virtual void SignalDeletedAttachment(const std::string& uuid,
                                         int32_t            contentType,
                                         uint64_t           uncompressedSize,
                                         const std::string& uncompressedHash,
                                         int32_t            compressionType,
                                         uint64_t           compressedSize,
                                         const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      Orthanc::DatabasePluginMessages::FileInfo* attachment;

      if (deleteAttachment_ != NULL)
      {
        if (deleteAttachment_->has_deleted_attachment())
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }
        
        attachment = deleteAttachment_->mutable_deleted_attachment();
      }
      else if (deleteResource_ != NULL)
      {
        attachment = deleteResource_->add_deleted_attachments();
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      
      attachment->set_uuid(uuid);
      attachment->set_content_type(contentType);
      attachment->set_uncompressed_size(uncompressedSize);
      attachment->set_uncompressed_hash(uncompressedHash);
      attachment->set_compression_type(compressionType);
      attachment->set_compressed_size(compressedSize);
      attachment->set_compressed_hash(compressedHash);
    }

    virtual void SignalDeletedResource(const std::string& publicId,
                                       OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE
    {
      if (deleteResource_ != NULL)
      {
        Orthanc::DatabasePluginMessages::DeleteResource_Response_Resource* resource = deleteResource_->add_deleted_resources();
        resource->set_level(Convert(resourceType));
        resource->set_public_id(publicId);
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
    }

    virtual void SignalRemainingAncestor(const std::string& ancestorId,
                                         OrthancPluginResourceType ancestorType) ORTHANC_OVERRIDE
    {
      if (deleteResource_ != NULL)
      {
        if (deleteResource_->is_remaining_ancestor())
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }
        else
        {
          deleteResource_->set_is_remaining_ancestor(true);
          deleteResource_->mutable_remaining_ancestor()->set_level(Convert(ancestorType));
          deleteResource_->mutable_remaining_ancestor()->set_public_id(ancestorId);
        }
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
    }
    
    virtual void AnswerAttachment(const std::string& uuid,
                                  int32_t            contentType,
                                  uint64_t           uncompressedSize,
                                  const std::string& uncompressedHash,
                                  int32_t            compressionType,
                                  uint64_t           compressedSize,
                                  const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    virtual void AnswerChange(int64_t                    seq,
                              int32_t                    changeType,
                              OrthancPluginResourceType  resourceType,
                              const std::string&         publicId,
                              const std::string&         date) ORTHANC_OVERRIDE
    {
      Orthanc::DatabasePluginMessages::ServerIndexChange* change;
      
      if (getChanges_ != NULL)
      {
        change = getChanges_->add_changes();
      }
      else if (getLastChange_ != NULL)
      {
        if (getLastChange_->exists())
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }

        getLastChange_->set_exists(true);
        change = getLastChange_->mutable_change();
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }

      change->set_seq(seq);
      change->set_change_type(changeType);
      change->set_resource_type(Convert(resourceType));
      change->set_public_id(publicId);
      change->set_date(date);
    }

    virtual void AnswerDicomTag(uint16_t group,
                                uint16_t element,
                                const std::string& value) ORTHANC_OVERRIDE
    {
      if (getMainDicomTags_ != NULL)
      {
        Orthanc::DatabasePluginMessages::GetMainDicomTags_Response_Tag* tag = getMainDicomTags_->add_tags();
        tag->set_key((static_cast<uint32_t>(group) << 16) + static_cast<uint32_t>(element));
        tag->set_value(value);
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
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
      Orthanc::DatabasePluginMessages::ExportedResource* resource;

      if (getExportedResources_ != NULL)
      {
        resource = getExportedResources_->add_resources();
      }
      else if (getLastExportedResource_ != NULL)
      {
        if (getLastExportedResource_->exists())
        {
          throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
        }

        getLastExportedResource_->set_exists(true);
        resource = getLastExportedResource_->mutable_resource();
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
      
      resource->set_seq(seq);
      resource->set_resource_type(Convert(resourceType));
      resource->set_public_id(publicId);
      resource->set_modality(modality);
      resource->set_date(date);
      resource->set_patient_id(patientId);
      resource->set_study_instance_uid(studyInstanceUid);
      resource->set_series_instance_uid(seriesInstanceUid);
      resource->set_sop_instance_uid(sopInstanceUid);
    }
    
    virtual void AnswerMatchingResource(const std::string& resourceId) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    
    virtual void AnswerMatchingResource(const std::string& resourceId,
                                        const std::string& someInstanceId) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
  };
  

  static void ProcessDatabaseOperation(Orthanc::DatabasePluginMessages::DatabaseResponse& response,
                                       const Orthanc::DatabasePluginMessages::DatabaseRequest& request,
                                       IndexConnectionsPool& pool)
  {
    switch (request.operation())
    {
      case Orthanc::DatabasePluginMessages::OPERATION_GET_SYSTEM_INFORMATION:
      {
        IndexConnectionsPool::Accessor accessor(pool);
        response.mutable_get_system_information()->set_database_version(accessor.GetBackend().GetDatabaseVersion(accessor.GetManager()));
        response.mutable_get_system_information()->set_supports_flush_to_disk(false);
        response.mutable_get_system_information()->set_supports_revisions(accessor.GetBackend().HasRevisionsSupport());
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_OPEN:
      {
        pool.OpenConnections();
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_CLOSE:
      {
        pool.CloseConnections();
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_FLUSH_TO_DISK:
      {
        // Raise an exception since "set_supports_flush_to_disk(false)"
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }

      case Orthanc::DatabasePluginMessages::OPERATION_START_TRANSACTION:
      {
        std::unique_ptr<IndexConnectionsPool::Accessor> transaction(new IndexConnectionsPool::Accessor(pool));

        switch (request.start_transaction().type())
        {
          case Orthanc::DatabasePluginMessages::TRANSACTION_READ_ONLY:
            transaction->GetManager().StartTransaction(TransactionType_ReadOnly);
            break;

          case Orthanc::DatabasePluginMessages::TRANSACTION_READ_WRITE:
            transaction->GetManager().StartTransaction(TransactionType_ReadWrite);
            break;

          default:
            throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
        }

        response.mutable_start_transaction()->set_transaction(reinterpret_cast<intptr_t>(transaction.release()));
        break;
      }
        
      case Orthanc::DatabasePluginMessages::OPERATION_UPGRADE:
      {
        IndexConnectionsPool::Accessor accessor(pool);
        OrthancPluginStorageArea* storageArea = reinterpret_cast<OrthancPluginStorageArea*>(request.upgrade().storage_area());
        accessor.GetBackend().UpgradeDatabase(accessor.GetManager(), request.upgrade().target_version(), storageArea);
        break;
      }
              
      default:
        LOG(ERROR) << "Not implemented database operation from protobuf: " << request.operation();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }

  
  static void ProcessTransactionOperation(Orthanc::DatabasePluginMessages::TransactionResponse& response,
                                          const Orthanc::DatabasePluginMessages::TransactionRequest& request,
                                          IndexBackend& backend,
                                          DatabaseManager& manager)
  {
    switch (request.operation())
    {
      case Orthanc::DatabasePluginMessages::OPERATION_ROLLBACK:
      {
        manager.RollbackTransaction();
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_COMMIT:
      {
        manager.CommitTransaction();
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_ADD_ATTACHMENT:
      {
        OrthancPluginAttachment attachment;
        attachment.uuid = request.add_attachment().attachment().uuid().c_str();
        attachment.contentType = request.add_attachment().attachment().content_type();
        attachment.uncompressedSize = request.add_attachment().attachment().uncompressed_size();
        attachment.uncompressedHash = request.add_attachment().attachment().uncompressed_hash().c_str();
        attachment.compressionType = request.add_attachment().attachment().compression_type();
        attachment.compressedSize = request.add_attachment().attachment().compressed_size();
        attachment.compressedHash = request.add_attachment().attachment().compressed_hash().c_str();
        
        backend.AddAttachment(manager, request.add_attachment().id(), attachment, request.add_attachment().revision());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_CLEAR_CHANGES:
      {
        backend.ClearChanges(manager);
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_CLEAR_EXPORTED_RESOURCES:
      {
        backend.ClearExportedResources(manager);
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_DELETE_ATTACHMENT:
      {
        Output output(*response.mutable_delete_attachment());
        backend.DeleteAttachment(output, manager, request.delete_attachment().id(), request.delete_attachment().type());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_DELETE_METADATA:
      {
        backend.DeleteMetadata(manager, request.delete_metadata().id(), request.delete_metadata().type());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_DELETE_RESOURCE:
      {
        response.mutable_delete_resource()->set_is_remaining_ancestor(false);

        Output output(*response.mutable_delete_resource());
        backend.DeleteResource(output, manager, request.delete_resource().id());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_ALL_METADATA:
      {
        typedef std::map<int32_t, std::string>  Values;

        Values values;
        backend.GetAllMetadata(values, manager, request.get_all_metadata().id());

        for (Values::const_iterator it = values.begin(); it != values.end(); ++it)
        {
          Orthanc::DatabasePluginMessages::GetAllMetadata_Response_Metadata* metadata =
            response.mutable_get_all_metadata()->add_metadata();
          metadata->set_type(it->first);
          metadata->set_value(it->second);
        }
        
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_ALL_PUBLIC_IDS:
      {
        std::list<std::string>  values;
        backend.GetAllPublicIds(values, manager, Convert(request.get_all_public_ids().resource_type()));

        for (std::list<std::string>::const_iterator it = values.begin(); it != values.end(); ++it)
        {
          response.mutable_get_all_public_ids()->add_ids(*it);
        }
        
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_ALL_PUBLIC_IDS_WITH_LIMITS:
      {
        std::list<std::string>  values;
        backend.GetAllPublicIds(values, manager, Convert(request.get_all_public_ids_with_limits().resource_type()),
                                request.get_all_public_ids_with_limits().since(),
                                request.get_all_public_ids_with_limits().limit());

        for (std::list<std::string>::const_iterator it = values.begin(); it != values.end(); ++it)
        {
          response.mutable_get_all_public_ids_with_limits()->add_ids(*it);
        }
        
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_CHANGES:
      {
        Output output(*response.mutable_get_changes());

        bool done;
        backend.GetChanges(output, done, manager, request.get_changes().since(), request.get_changes().limit());

        response.mutable_get_changes()->set_done(done);
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_CHILDREN_INTERNAL_ID:
      {
        std::list<int64_t>  values;
        backend.GetChildrenInternalId(values, manager, request.get_children_internal_id().id());

        for (std::list<int64_t>::const_iterator it = values.begin(); it != values.end(); ++it)
        {
          response.mutable_get_children_internal_id()->add_ids(*it);
        }
        
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_CHILDREN_PUBLIC_ID:
      {
        std::list<std::string>  values;
        backend.GetChildrenPublicId(values, manager, request.get_children_public_id().id());

        for (std::list<std::string>::const_iterator it = values.begin(); it != values.end(); ++it)
        {
          response.mutable_get_children_public_id()->add_ids(*it);
        }
        
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_EXPORTED_RESOURCES:
      {
        Output output(*response.mutable_get_exported_resources());

        bool done;
        backend.GetExportedResources(output, done, manager, request.get_exported_resources().since(),
                                     request.get_exported_resources().limit());

        response.mutable_get_exported_resources()->set_done(done);
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_GET_LAST_CHANGE:
      {
        response.mutable_get_last_change()->set_exists(false);

        Output output(*response.mutable_get_last_change());
        backend.GetLastChange(output, manager);
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_GET_LAST_EXPORTED_RESOURCE:
      {
        response.mutable_get_last_exported_resource()->set_exists(false);

        Output output(*response.mutable_get_last_exported_resource());
        backend.GetLastExportedResource(output, manager);
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_GET_MAIN_DICOM_TAGS:
      {
        Output output(*response.mutable_get_main_dicom_tags());
        backend.GetMainDicomTags(output, manager, request.get_main_dicom_tags().id());
        break;
      }

      case Orthanc::DatabasePluginMessages::OPERATION_GET_PUBLIC_ID:
      {
        const std::string id = backend.GetPublicId(manager, request.get_public_id().id());
        response.mutable_get_public_id()->set_id(id);
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_RESOURCES_COUNT:
      {
        OrthancPluginResourceType type = Convert(request.get_resources_count().type());
        uint64_t count = backend.GetResourcesCount(manager, type);
        response.mutable_get_resources_count()->set_count(count);
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_RESOURCE_TYPE:
      {
        OrthancPluginResourceType type = backend.GetResourceType(manager, request.get_public_id().id());
        response.mutable_get_resource_type()->set_type(Convert(type));
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_TOTAL_COMPRESSED_SIZE:
      {
        response.mutable_get_total_compressed_size()->set_size(backend.GetTotalCompressedSize(manager));
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_GET_TOTAL_UNCOMPRESSED_SIZE:
      {
        response.mutable_get_total_uncompressed_size()->set_size(backend.GetTotalUncompressedSize(manager));
        break;
      }
      
      default:
        LOG(ERROR) << "Not implemented transaction operation from protobuf: " << request.operation();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }

  
  static OrthancPluginErrorCode CallBackend(OrthancPluginMemoryBuffer64* serializedResponse,
                                            void* rawPool,
                                            const void* requestData,
                                            uint64_t requestSize)
  {
    Orthanc::DatabasePluginMessages::Request request;
    if (!request.ParseFromArray(requestData, requestSize))
    {
      LOG(ERROR) << "Cannot parse message from the Orthanc core using protobuf";
      return OrthancPluginErrorCode_InternalError;
    }

    if (rawPool == NULL)
    {
      LOG(ERROR) << "Received a NULL pointer from the database";
      return OrthancPluginErrorCode_InternalError;
    }

    IndexConnectionsPool& pool = *reinterpret_cast<IndexConnectionsPool*>(rawPool);

    try
    {
      Orthanc::DatabasePluginMessages::Response response;
      
      switch (request.type())
      {
        case Orthanc::DatabasePluginMessages::REQUEST_DATABASE:
          ProcessDatabaseOperation(*response.mutable_database_response(), request.database_request(), pool);
          break;
          
        case Orthanc::DatabasePluginMessages::REQUEST_TRANSACTION:
        {
          IndexConnectionsPool::Accessor& transaction = *reinterpret_cast<IndexConnectionsPool::Accessor*>(request.transaction_request().transaction());
          ProcessTransactionOperation(*response.mutable_transaction_response(), request.transaction_request(),
                                      transaction.GetBackend(), transaction.GetManager());
          break;
        }
          
        default:
          LOG(ERROR) << "Not implemented request type from protobuf: " << request.type();
          break;
      }

      std::string s;
      if (!response.SerializeToString(&s))
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError, "Cannot serialize to protobuf");
      }

      if (OrthancPluginCreateMemoryBuffer64(pool.GetContext(), serializedResponse, s.size()) != OrthancPluginErrorCode_Success)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotEnoughMemory, "Cannot allocate a memory buffer");
      }

      if (!s.empty())
      {
        assert(serializedResponse->size == s.size());
        memcpy(serializedResponse->data, s.c_str(), s.size());
      }

      return OrthancPluginErrorCode_Success;
    }
    catch (::Orthanc::OrthancException& e)
    {
      LOG(ERROR) << "Exception in database back-end: " << e.What();
      return static_cast<OrthancPluginErrorCode>(e.GetErrorCode());
    }
    catch (::std::runtime_error& e)
    {
      LOG(ERROR) << "Exception in database back-end: " << e.what();
      return OrthancPluginErrorCode_DatabasePlugin;
    }
    catch (...)
    {
      LOG(ERROR) << "Native exception";
      return OrthancPluginErrorCode_DatabasePlugin;
    }
  }

  static void FinalizeBackend(void* rawPool)
  {
    if (rawPool != NULL)
    {
      IndexConnectionsPool* pool = reinterpret_cast<IndexConnectionsPool*>(rawPool);
      
      if (isBackendInUse_)
      {
        isBackendInUse_ = false;
      }
      else
      {
        LOG(ERROR) << "More than one index backend was registered, internal error";
      }

      delete pool;
    }
    else
    {
      LOG(ERROR) << "Received a null pointer from the Orthanc core, internal error";
    }
  }

  
  void DatabaseBackendAdapterV4::Register(IndexBackend* backend,
                                          size_t countConnections,
                                          unsigned int maxDatabaseRetries)
  {
    std::unique_ptr<IndexConnectionsPool> pool(new IndexConnectionsPool(backend, countConnections));
    
    if (isBackendInUse_)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    OrthancPluginContext* context = backend->GetContext();
 
    if (OrthancPluginRegisterDatabaseBackendV4(context, pool.release(), maxDatabaseRetries,
                                               CallBackend, FinalizeBackend) != OrthancPluginErrorCode_Success)
    {
      delete backend;
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError, "Unable to register the database backend");
    }

    isBackendInUse_ = true;
  }


  void DatabaseBackendAdapterV4::Finalize()
  {
    if (isBackendInUse_)
    {
      LOG(ERROR) << "The Orthanc core has not destructed the index backend, internal error";
    }
  }
}

#  endif
#endif
