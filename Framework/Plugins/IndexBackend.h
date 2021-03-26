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

#include "../Common/DatabaseManager.h"
#include "IDatabaseBackend.h"

#include <OrthancException.h>


namespace OrthancDatabases
{
  class IndexBackend : public IDatabaseBackend
  {
  private:
    class LookupFormatter;

    OrthancPluginContext*  context_;
    DatabaseManager        manager_;

    std::unique_ptr<IDatabaseBackendOutput::IFactory>  outputFactory_;
    
  protected:
    DatabaseManager& GetManager()
    {
      return manager_;
    }
    
    static int64_t ReadInteger64(const DatabaseManager::StatementBase& statement,
                                 size_t field);

    static int32_t ReadInteger32(const DatabaseManager::StatementBase& statement,
                                 size_t field);
    
    static std::string ReadString(const DatabaseManager::StatementBase& statement,
                                  size_t field);
    
    template <typename T>
    static void ReadListOfIntegers(std::list<T>& target,
                                   DatabaseManager::CachedStatement& statement,
                                   const Dictionary& args);
    
    static void ReadListOfStrings(std::list<std::string>& target,
                                  DatabaseManager::CachedStatement& statement,
                                  const Dictionary& args);

    void ClearDeletedFiles();

    void ClearDeletedResources();

    void SignalDeletedFiles(IDatabaseBackendOutput& output);

    void SignalDeletedResources(IDatabaseBackendOutput& output);

  private:
    void ReadChangesInternal(IDatabaseBackendOutput& output,
                             bool& done,
                             DatabaseManager::CachedStatement& statement,
                             const Dictionary& args,
                             uint32_t maxResults);

    void ReadExportedResourcesInternal(IDatabaseBackendOutput& output,
                                       bool& done,
                                       DatabaseManager::CachedStatement& statement,
                                       const Dictionary& args,
                                       uint32_t maxResults);

  public:
    IndexBackend(OrthancPluginContext* context,
                 IDatabaseFactory* factory);

    virtual OrthancPluginContext* GetContext() ORTHANC_OVERRIDE
    {
      return context_;
    }

    virtual void SetOutputFactory(IDatabaseBackendOutput::IFactory* factory) ORTHANC_OVERRIDE;
    
    virtual IDatabaseBackendOutput* CreateOutput() ORTHANC_OVERRIDE;
    
    virtual void Open() ORTHANC_OVERRIDE
    {
      manager_.Open();
    }
    
    virtual void Close() ORTHANC_OVERRIDE
    {
      manager_.Close();
    }
    
    virtual void AddAttachment(int64_t id,
                               const OrthancPluginAttachment& attachment) ORTHANC_OVERRIDE;
    
    virtual void AttachChild(int64_t parent,
                             int64_t child) ORTHANC_OVERRIDE;
    
    virtual void ClearChanges() ORTHANC_OVERRIDE;
    
    virtual void ClearExportedResources() ORTHANC_OVERRIDE;

    virtual void DeleteAttachment(IDatabaseBackendOutput& output,
                                  int64_t id,
                                  int32_t attachment) ORTHANC_OVERRIDE;
    
    virtual void DeleteMetadata(int64_t id,
                                int32_t metadataType) ORTHANC_OVERRIDE;
    
    virtual void DeleteResource(IDatabaseBackendOutput& output,
                                int64_t id) ORTHANC_OVERRIDE;

    virtual void GetAllInternalIds(std::list<int64_t>& target,
                                   OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE;
    
    virtual void GetAllPublicIds(std::list<std::string>& target,
                                 OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE;
    
    virtual void GetAllPublicIds(std::list<std::string>& target,
                                 OrthancPluginResourceType resourceType,
                                 uint64_t since,
                                 uint64_t limit) ORTHANC_OVERRIDE;
    
    virtual void GetChanges(IDatabaseBackendOutput& output,
                            bool& done /*out*/,
                            int64_t since,
                            uint32_t maxResults) ORTHANC_OVERRIDE;
    
    virtual void GetChildrenInternalId(std::list<int64_t>& target /*out*/,
                                       int64_t id) ORTHANC_OVERRIDE;
    
    virtual void GetChildrenPublicId(std::list<std::string>& target /*out*/,
                                     int64_t id) ORTHANC_OVERRIDE;
    
    virtual void GetExportedResources(IDatabaseBackendOutput& output,
                                      bool& done /*out*/,
                                      int64_t since,
                                      uint32_t maxResults) ORTHANC_OVERRIDE;
    
    virtual void GetLastChange(IDatabaseBackendOutput& output) ORTHANC_OVERRIDE;
    
    virtual void GetLastExportedResource(IDatabaseBackendOutput& output) ORTHANC_OVERRIDE;
    
    virtual void GetMainDicomTags(IDatabaseBackendOutput& output,
                                  int64_t id) ORTHANC_OVERRIDE;
    
    virtual std::string GetPublicId(int64_t resourceId) ORTHANC_OVERRIDE;
    
    virtual uint64_t GetResourcesCount(OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE;
    
    virtual OrthancPluginResourceType GetResourceType(int64_t resourceId) ORTHANC_OVERRIDE;
    
    virtual uint64_t GetTotalCompressedSize() ORTHANC_OVERRIDE;
    
    virtual uint64_t GetTotalUncompressedSize() ORTHANC_OVERRIDE;
    
    virtual bool IsExistingResource(int64_t internalId) ORTHANC_OVERRIDE;
    
    virtual bool IsProtectedPatient(int64_t internalId) ORTHANC_OVERRIDE;
    
    virtual void ListAvailableMetadata(std::list<int32_t>& target /*out*/,
                                       int64_t id) ORTHANC_OVERRIDE;
    
    virtual void ListAvailableAttachments(std::list<int32_t>& target /*out*/,
                                          int64_t id) ORTHANC_OVERRIDE;
    
    virtual void LogChange(int32_t changeType,
                           int64_t resourceId,
                           OrthancPluginResourceType resourceType,
                           const char* date) ORTHANC_OVERRIDE;
    
    virtual void LogExportedResource(const OrthancPluginExportedResource& resource) ORTHANC_OVERRIDE;
    
    virtual bool LookupAttachment(IDatabaseBackendOutput& output,
                                  int64_t id,
                                  int32_t contentType) ORTHANC_OVERRIDE;
    
    virtual bool LookupGlobalProperty(std::string& target /*out*/,
                                      int32_t property) ORTHANC_OVERRIDE;
    
    virtual void LookupIdentifier(std::list<int64_t>& target /*out*/,
                                  OrthancPluginResourceType resourceType,
                                  uint16_t group,
                                  uint16_t element,
                                  OrthancPluginIdentifierConstraint constraint,
                                  const char* value) ORTHANC_OVERRIDE;
    
    virtual void LookupIdentifierRange(std::list<int64_t>& target /*out*/,
                                       OrthancPluginResourceType resourceType,
                                       uint16_t group,
                                       uint16_t element,
                                       const char* start,
                                       const char* end) ORTHANC_OVERRIDE;

    virtual bool LookupMetadata(std::string& target /*out*/,
                                int64_t id,
                                int32_t metadataType) ORTHANC_OVERRIDE;

    virtual bool LookupParent(int64_t& parentId /*out*/,
                              int64_t resourceId) ORTHANC_OVERRIDE;
    
    virtual bool LookupResource(int64_t& id /*out*/,
                                OrthancPluginResourceType& type /*out*/,
                                const char* publicId) ORTHANC_OVERRIDE;
    
    virtual bool SelectPatientToRecycle(int64_t& internalId /*out*/) ORTHANC_OVERRIDE;
    
    virtual bool SelectPatientToRecycle(int64_t& internalId /*out*/,
                                        int64_t patientIdToAvoid) ORTHANC_OVERRIDE;
    
    virtual void SetGlobalProperty(int32_t property,
                                   const char* value) ORTHANC_OVERRIDE;

    virtual void SetMainDicomTag(int64_t id,
                                 uint16_t group,
                                 uint16_t element,
                                 const char* value) ORTHANC_OVERRIDE;
    
    virtual void SetIdentifierTag(int64_t id,
                                  uint16_t group,
                                  uint16_t element,
                                  const char* value) ORTHANC_OVERRIDE;

    virtual void SetMetadata(int64_t id,
                             int32_t metadataType,
                             const char* value) ORTHANC_OVERRIDE;
    
    virtual void SetProtectedPatient(int64_t internalId, 
                                     bool isProtected) ORTHANC_OVERRIDE;
    
    virtual void StartTransaction(TransactionType type) ORTHANC_OVERRIDE
    {
      manager_.StartTransaction(type);
    }

    
    virtual void RollbackTransaction() ORTHANC_OVERRIDE
    {
      manager_.RollbackTransaction();
    }

    
    virtual void CommitTransaction() ORTHANC_OVERRIDE
    {
      manager_.CommitTransaction();
    }

    
    virtual uint32_t GetDatabaseVersion() ORTHANC_OVERRIDE;
    
    virtual void UpgradeDatabase(uint32_t  targetVersion,
                                 OrthancPluginStorageArea* storageArea) ORTHANC_OVERRIDE;
    
    virtual void ClearMainDicomTags(int64_t internalId) ORTHANC_OVERRIDE;

    // For unit testing only!
    virtual uint64_t GetAllResourcesCount();

    // For unit testing only!
    virtual uint64_t GetUnprotectedPatientsCount();

    // For unit testing only!
    virtual bool GetParentPublicId(std::string& target,
                                   int64_t id);

    // For unit tests only!
    virtual void GetChildren(std::list<std::string>& childrenPublicIds,
                             int64_t id);

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    // New primitive since Orthanc 1.5.2
    virtual void LookupResources(IDatabaseBackendOutput& output,
                                 const std::vector<Orthanc::DatabaseConstraint>& lookup,
                                 OrthancPluginResourceType queryLevel,
                                 uint32_t limit,
                                 bool requestSomeInstance) ORTHANC_OVERRIDE;
#endif

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    // New primitive since Orthanc 1.5.2
    virtual void SetResourcesContent(
      uint32_t countIdentifierTags,
      const OrthancPluginResourcesContentTags* identifierTags,
      uint32_t countMainDicomTags,
      const OrthancPluginResourcesContentTags* mainDicomTags,
      uint32_t countMetadata,
      const OrthancPluginResourcesContentMetadata* metadata) ORTHANC_OVERRIDE;
#endif

    // New primitive since Orthanc 1.5.2
    virtual void GetChildrenMetadata(std::list<std::string>& target,
                                     int64_t resourceId,
                                     int32_t metadata) ORTHANC_OVERRIDE;

    virtual void TagMostRecentPatient(int64_t patient) ORTHANC_OVERRIDE;

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
    // New primitive since Orthanc 1.5.4
    virtual bool LookupResourceAndParent(int64_t& id,
                                         OrthancPluginResourceType& type,
                                         std::string& parentPublicId,
                                         const char* publicId) ORTHANC_OVERRIDE;
#  endif
#endif

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
    // New primitive since Orthanc 1.5.4
    virtual void GetAllMetadata(std::map<int32_t, std::string>& result,
                                int64_t id) ORTHANC_OVERRIDE;
#  endif
#endif

    virtual bool HasCreateInstance() const ORTHANC_OVERRIDE
    {
      // This extension is available in PostgreSQL and MySQL, but is
      // emulated by "CreateInstanceGeneric()" in SQLite
      return false;
    }
      
#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
    virtual void CreateInstance(OrthancPluginCreateInstanceResult& result,
                                const char* hashPatient,
                                const char* hashStudy,
                                const char* hashSeries,
                                const char* hashInstance) ORTHANC_OVERRIDE
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
#endif

    // This function corresponds to
    // "Orthanc::Compatibility::ICreateInstance::Apply()"
    void CreateInstanceGeneric(OrthancPluginCreateInstanceResult& result,
                               const char* hashPatient,
                               const char* hashStudy,
                               const char* hashSeries,
                               const char* hashInstance);

    static void Register(IndexBackend& backend);
  };
}
