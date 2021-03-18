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

#include "IDatabaseBackendOutput.h"

#include <list>

namespace OrthancPlugins
{
  class IDatabaseBackend : public boost::noncopyable
  {
  public:
    virtual ~IDatabaseBackend()
    {
    }

    virtual OrthancPluginContext* GetContext() = 0;

    virtual void SetOutputFactory(IDatabaseBackendOutput::IFactory* factory) = 0;
                        
    virtual IDatabaseBackendOutput* CreateOutput() = 0;

    virtual void Open() = 0;

    virtual void Close() = 0;

    virtual void AddAttachment(int64_t id,
                               const OrthancPluginAttachment& attachment) = 0;

    virtual void AttachChild(int64_t parent,
                             int64_t child) = 0;

    virtual void ClearChanges() = 0;

    virtual void ClearExportedResources() = 0;

    virtual int64_t CreateResource(const char* publicId,
                                   OrthancPluginResourceType type) = 0;

    virtual void DeleteAttachment(OrthancPlugins::IDatabaseBackendOutput& output,
                                  int64_t id,
                                  int32_t attachment) = 0;

    virtual void DeleteMetadata(int64_t id,
                                int32_t metadataType) = 0;

    virtual void DeleteResource(OrthancPlugins::IDatabaseBackendOutput& output,
                                int64_t id) = 0;

    virtual void GetAllInternalIds(std::list<int64_t>& target,
                                   OrthancPluginResourceType resourceType) = 0;

    virtual void GetAllPublicIds(std::list<std::string>& target,
                                 OrthancPluginResourceType resourceType) = 0;

    virtual void GetAllPublicIds(std::list<std::string>& target,
                                 OrthancPluginResourceType resourceType,
                                 uint64_t since,
                                 uint64_t limit) = 0;

    /* Use GetOutput().AnswerChange() */
    virtual void GetChanges(OrthancPlugins::IDatabaseBackendOutput& output,
                            bool& done /*out*/,
                            int64_t since,
                            uint32_t maxResults) = 0;

    virtual void GetChildrenInternalId(std::list<int64_t>& target /*out*/,
                                       int64_t id) = 0;

    virtual void GetChildrenPublicId(std::list<std::string>& target /*out*/,
                                     int64_t id) = 0;

    /* Use GetOutput().AnswerExportedResource() */
    virtual void GetExportedResources(OrthancPlugins::IDatabaseBackendOutput& output,
                                      bool& done /*out*/,
                                      int64_t since,
                                      uint32_t maxResults) = 0;

    /* Use GetOutput().AnswerChange() */
    virtual void GetLastChange(OrthancPlugins::IDatabaseBackendOutput& output) = 0;

    /* Use GetOutput().AnswerExportedResource() */
    virtual void GetLastExportedResource(OrthancPlugins::IDatabaseBackendOutput& output) = 0;

    /* Use GetOutput().AnswerDicomTag() */
    virtual void GetMainDicomTags(OrthancPlugins::IDatabaseBackendOutput& output,
                                  int64_t id) = 0;

    virtual std::string GetPublicId(int64_t resourceId) = 0;

    virtual uint64_t GetResourceCount(OrthancPluginResourceType resourceType) = 0;

    virtual OrthancPluginResourceType GetResourceType(int64_t resourceId) = 0;

    virtual uint64_t GetTotalCompressedSize() = 0;
    
    virtual uint64_t GetTotalUncompressedSize() = 0;

    virtual bool IsExistingResource(int64_t internalId) = 0;

    virtual bool IsProtectedPatient(int64_t internalId) = 0;

    virtual void ListAvailableMetadata(std::list<int32_t>& target /*out*/,
                                       int64_t id) = 0;

    virtual void ListAvailableAttachments(std::list<int32_t>& target /*out*/,
                                          int64_t id) = 0;

    virtual void LogChange(const OrthancPluginChange& change) = 0;

    virtual void LogExportedResource(const OrthancPluginExportedResource& resource) = 0;
    
    /* Use GetOutput().AnswerAttachment() */
    virtual bool LookupAttachment(OrthancPlugins::IDatabaseBackendOutput& output,
                                  int64_t id,
                                  int32_t contentType) = 0;

    virtual bool LookupGlobalProperty(std::string& target /*out*/,
                                      int32_t property) = 0;

    virtual void LookupIdentifier(std::list<int64_t>& target /*out*/,
                                  OrthancPluginResourceType resourceType,
                                  uint16_t group,
                                  uint16_t element,
                                  OrthancPluginIdentifierConstraint constraint,
                                  const char* value) = 0;

    virtual void LookupIdentifierRange(std::list<int64_t>& target /*out*/,
                                       OrthancPluginResourceType resourceType,
                                       uint16_t group,
                                       uint16_t element,
                                       const char* start,
                                       const char* end) = 0;

    virtual bool LookupMetadata(std::string& target /*out*/,
                                int64_t id,
                                int32_t metadataType) = 0;

    virtual bool LookupParent(int64_t& parentId /*out*/,
                              int64_t resourceId) = 0;

    virtual bool LookupResource(int64_t& id /*out*/,
                                OrthancPluginResourceType& type /*out*/,
                                const char* publicId) = 0;

    virtual bool SelectPatientToRecycle(int64_t& internalId /*out*/) = 0;

    virtual bool SelectPatientToRecycle(int64_t& internalId /*out*/,
                                        int64_t patientIdToAvoid) = 0;

    virtual void SetGlobalProperty(int32_t property,
                                   const char* value) = 0;

    virtual void SetMainDicomTag(int64_t id,
                                 uint16_t group,
                                 uint16_t element,
                                 const char* value) = 0;

    virtual void SetIdentifierTag(int64_t id,
                                  uint16_t group,
                                  uint16_t element,
                                  const char* value) = 0;

    virtual void SetMetadata(int64_t id,
                             int32_t metadataType,
                             const char* value) = 0;

    virtual void SetProtectedPatient(int64_t internalId, 
                                     bool isProtected) = 0;

    virtual void StartTransaction() = 0;

    virtual void RollbackTransaction() = 0;

    virtual void CommitTransaction() = 0;

    virtual uint32_t GetDatabaseVersion() = 0;

    /**
     * Upgrade the database to the specified version of the database
     * schema.  The upgrade script is allowed to make calls to
     * OrthancPluginReconstructMainDicomTags().
     **/
    virtual void UpgradeDatabase(uint32_t  targetVersion,
                                 OrthancPluginStorageArea* storageArea) = 0;

    virtual void ClearMainDicomTags(int64_t internalId) = 0;

    virtual bool HasCreateInstance() const = 0;

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void LookupResources(OrthancPlugins::IDatabaseBackendOutput& output,
                                 const std::vector<Orthanc::DatabaseConstraint>& lookup,
                                 OrthancPluginResourceType queryLevel,
                                 uint32_t limit,
                                 bool requestSomeInstance) = 0;
#endif

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void CreateInstance(OrthancPluginCreateInstanceResult& result,
                                const char* hashPatient,
                                const char* hashStudy,
                                const char* hashSeries,
                                const char* hashInstance) = 0;
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void SetResourcesContent(
      uint32_t countIdentifierTags,
      const OrthancPluginResourcesContentTags* identifierTags,
      uint32_t countMainDicomTags,
      const OrthancPluginResourcesContentTags* mainDicomTags,
      uint32_t countMetadata,
      const OrthancPluginResourcesContentMetadata* metadata) = 0;
#endif

    
    virtual void GetChildrenMetadata(std::list<std::string>& target,
                                     int64_t resourceId,
                                     int32_t metadata) = 0;

    virtual int64_t GetLastChangeIndex() = 0;

    virtual void TagMostRecentPatient(int64_t patientId) = 0;

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
    // NB: "parentPublicId" must be cleared if the resource has no parent
    virtual bool LookupResourceAndParent(int64_t& id,
                                         OrthancPluginResourceType& type,
                                         std::string& parentPublicId,
                                         const char* publicId) = 0;
#  endif
#endif

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
    virtual void GetAllMetadata(std::map<int32_t, std::string>& result,
                                int64_t id) = 0;
#  endif
#endif
  };
}
