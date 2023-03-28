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


  class Output : public IDatabaseBackendOutput
  {
  private:
    Orthanc::DatabasePluginMessages::DeleteAttachment::Response*  deleteAttachment_;

    void Clear()
    {
      deleteAttachment_ = NULL;
    }
    
  public:
    Output(Orthanc::DatabasePluginMessages::DeleteAttachment::Response& deleteAttachment)
    {
      Clear();
      deleteAttachment_ = &deleteAttachment;
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
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    virtual void SignalRemainingAncestor(const std::string& ancestorId,
                                         OrthancPluginResourceType ancestorType) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
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
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }

    virtual void AnswerDicomTag(uint16_t group,
                                uint16_t element,
                                const std::string& value) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
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
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
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
                                          IndexConnectionsPool::Accessor& transaction)
  {
    switch (request.operation())
    {
      case Orthanc::DatabasePluginMessages::OPERATION_ROLLBACK:
      {
        transaction.GetManager().RollbackTransaction();
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_COMMIT:
      {
        transaction.GetManager().CommitTransaction();
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
        
        transaction.GetBackend().AddAttachment(transaction.GetManager(), request.add_attachment().id(), attachment,
                                               request.add_attachment().revision());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_CLEAR_CHANGES:
      {
        transaction.GetBackend().ClearChanges(transaction.GetManager());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_CLEAR_EXPORTED_RESOURCES:
      {
        transaction.GetBackend().ClearExportedResources(transaction.GetManager());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_DELETE_ATTACHMENT:
      {
        Output output(*response.mutable_delete_attachment());
        transaction.GetBackend().DeleteAttachment(
          output, transaction.GetManager(), request.delete_attachment().id(), request.delete_attachment().type());
        break;
      }
      
      case Orthanc::DatabasePluginMessages::OPERATION_DELETE_METADATA:
      {
        transaction.GetBackend().DeleteMetadata(
          transaction.GetManager(), request.delete_metadata().id(), request.delete_metadata().type());
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
          ProcessTransactionOperation(*response.mutable_transaction_response(), request.transaction_request(), transaction);
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
