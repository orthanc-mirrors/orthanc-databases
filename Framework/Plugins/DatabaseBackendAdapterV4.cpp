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

#include <OrthancDatabasePlugin.pb.h>  // Include protobuf messages

#include <Logging.h>
#include <MultiThreading/SharedMessageQueue.h>
#include <OrthancException.h>

#include <stdexcept>
#include <list>
#include <string>
#include <cassert>


#define ORTHANC_PLUGINS_DATABASE_CATCH(context)                         \


namespace OrthancDatabases
{
  static bool isBackendInUse_ = false;  // Only for sanity checks

  
  static void ProcessDatabaseOperation(Orthanc::DatabasePluginMessages::DatabaseResponse& response,
                                       const Orthanc::DatabasePluginMessages::DatabaseRequest& request,
                                       IndexBackend& backend)
  {
    switch (request.operation())
    {
      case Orthanc::DatabasePluginMessages::OPERATION_GET_SYSTEM_INFORMATION:
        response.mutable_get_system_information()->set_supports_revisions(backend.HasRevisionsSupport());
        break;
              
      default:
        LOG(ERROR) << "Not implemented database operation from protobuf: " << request.operation();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }

  
  static void ProcessTransactionOperation(Orthanc::DatabasePluginMessages::TransactionResponse& response,
                                          const Orthanc::DatabasePluginMessages::TransactionRequest& request,
                                          IndexBackend& backend)
  {
    switch (request.operation())
    {
      default:
        LOG(ERROR) << "Not implemented transaction operation from protobuf: " << request.operation();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
    }
  }

  
  static OrthancPluginErrorCode CallBackend(OrthancPluginMemoryBuffer64* serializedResponse,
                                            void* rawBackend,
                                            const void* requestData,
                                            uint64_t requestSize)
  {
    Orthanc::DatabasePluginMessages::Request request;
    if (!request.ParseFromArray(requestData, requestSize))
    {
      LOG(ERROR) << "Cannot parse message from the Orthanc core using protobuf";
      return OrthancPluginErrorCode_InternalError;
    }

    if (rawBackend == NULL)
    {
      LOG(ERROR) << "Received a NULL pointer from the database";
      return OrthancPluginErrorCode_InternalError;
    }

    IndexBackend& backend = *reinterpret_cast<IndexBackend*>(rawBackend);

    try
    {
      Orthanc::DatabasePluginMessages::Response response;
      
      switch (request.type())
      {
        case Orthanc::DatabasePluginMessages::REQUEST_DATABASE:
          ProcessDatabaseOperation(*response.mutable_database_response(), request.database_request(), backend);
          break;
          
        case Orthanc::DatabasePluginMessages::REQUEST_TRANSACTION:
          ProcessTransactionOperation(*response.mutable_transaction_response(), request.transaction_request(), backend);
          break;
          
        default:
          LOG(ERROR) << "Not implemented request type from protobuf: " << request.type();
          break;
      }

      std::string s;
      if (!response.SerializeToString(&s))
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError, "Cannot serialize to protobuf");
      }

      if (OrthancPluginCreateMemoryBuffer64(backend.GetContext(), serializedResponse, s.size()) != OrthancPluginErrorCode_Success)
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

  static void FinalizeBackend(void* rawBackend)
  {
    if (rawBackend != NULL)
    {
      IndexBackend* backend = reinterpret_cast<IndexBackend*>(rawBackend);
      
      if (isBackendInUse_)
      {
        isBackendInUse_ = false;
      }
      else
      {
        LOG(ERROR) << "More than one index backend was registered, internal error";
      }

      delete backend;
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
    std::unique_ptr<IndexBackend> protection(backend);
    
    if (isBackendInUse_)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    
    if (backend == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }

    OrthancPluginContext* context = backend->GetContext();
 
    if (OrthancPluginRegisterDatabaseBackendV4(context, protection.release(), maxDatabaseRetries,
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
