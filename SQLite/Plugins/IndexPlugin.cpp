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


#include "SQLiteIndex.h"
#include "../../Framework/Plugins/PluginInitialization.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>

static std::unique_ptr<OrthancDatabases::SQLiteIndex> backend_;



#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 10, 0)


#define ORTHANC_PLUGINS_DATABASE_CATCH(context)                         \
  catch (::Orthanc::OrthancException& e)                                \
  {                                                                     \
    return static_cast<OrthancPluginErrorCode>(e.GetErrorCode());       \
  }                                                                     \
  catch (::std::runtime_error& e)                                       \
  {                                                                     \
    const std::string message = "Exception in database back-end: " + std::string(e.what()); \
    OrthancPluginLogError(context, message.c_str());                    \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }                                                                     \
  catch (...)                                                           \
  {                                                                     \
    OrthancPluginLogError(context, "Native exception");                 \
    return OrthancPluginErrorCode_DatabasePlugin;                       \
  }


namespace OrthancDatabases
{
  class Output : public IDatabaseBackendOutput
  {
  private:
    struct Metadata
    {
      int32_t      metadata;
      const char*  value;
    };
    
    _OrthancPluginDatabaseAnswerType            answerType_;
    std::list<std::string>                      stringsStore_;
    
    std::vector<OrthancPluginAttachment>        attachments_;
    std::vector<OrthancPluginChange>            changes_;
    std::vector<OrthancPluginDicomTag>          tags_;
    std::vector<OrthancPluginExportedResource>  exported_;
    std::vector<OrthancPluginDatabaseEvent>     events_;
    std::vector<int32_t>                        integers32_;
    std::vector<int64_t>                        integers64_;
    std::vector<OrthancPluginMatchingResource>  matches_;
    std::vector<Metadata>                       metadata_;
    std::vector<std::string>                    stringAnswers_;
    
    const char* StoreString(const std::string& s)
    {
      stringsStore_.push_back(s);
      return stringsStore_.back().c_str();
    }

    void SetupAnswerType(_OrthancPluginDatabaseAnswerType type)
    {
      if (answerType_ == _OrthancPluginDatabaseAnswerType_None)
      {
        answerType_ = type;
      }
      else if (answerType_ != type)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
    }
    
  public:
    Output() :
      answerType_(_OrthancPluginDatabaseAnswerType_None)
    {
    }
    
    
    void Clear()
    {
      answerType_ = _OrthancPluginDatabaseAnswerType_None;
      stringsStore_.clear();
      
      attachments_.clear();
      changes_.clear();
      tags_.clear();
      exported_.clear();
      events_.clear();
      integers32_.clear();
      integers64_.clear();
      matches_.clear();
      metadata_.clear();
      stringAnswers_.clear();
    }


    static OrthancPluginErrorCode ReadAnswersCount(OrthancPluginDatabaseTransaction* transaction,
                                                   uint32_t* target /* out */)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      size_t size;
      
      switch (that.answerType_)
      {
        case _OrthancPluginDatabaseAnswerType_None:
          size = 0;
          break;
        
        case _OrthancPluginDatabaseAnswerType_Attachment:
          size = that.attachments_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Change:
          size = that.changes_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_DicomTag:
          size = that.tags_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_ExportedResource:
          size = that.exported_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int32:
          size = that.integers32_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int64:
          size = that.integers64_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_MatchingResource:
          size = that.matches_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Metadata:
          size = that.metadata_.size();
          break;
        
        case _OrthancPluginDatabaseAnswerType_String:
          size = that.stringAnswers_.size();
          break;
        
        default:
          return OrthancPluginErrorCode_InternalError;
      }

      *target = static_cast<uint32_t>(size);
      return OrthancPluginErrorCode_Success;
    }


    static OrthancPluginErrorCode ReadAnswerAttachment(OrthancPluginDatabaseTransaction* transaction,
                                                       OrthancPluginAttachment* target /* out */,
                                                       uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.attachments_.size())
      {
        *target = that.attachments_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerChange(OrthancPluginDatabaseTransaction* transaction,
                                                       OrthancPluginChange* target /* out */,
                                                       uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.changes_.size())
      {
        *target = that.changes_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerDicomTag(OrthancPluginDatabaseTransaction* transaction,
                                                     uint16_t* group,
                                                     uint16_t* element,
                                                     const char** value,
                                                     uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.tags_.size())
      {
        const OrthancPluginDicomTag& tag = that.tags_[index];
        *group = tag.group;
        *element = tag.element;
        *value = tag.value;
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerExportedResource(OrthancPluginDatabaseTransaction* transaction,
                                                             OrthancPluginExportedResource* target /* out */,
                                                             uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.exported_.size())
      {
        *target = that.exported_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerInt32(OrthancPluginDatabaseTransaction* transaction,
                                                  int32_t* target,
                                                  uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.integers32_.size())
      {
        *target = that.integers32_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerInt64(OrthancPluginDatabaseTransaction* transaction,
                                                  int64_t* target,
                                                  uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.integers64_.size())
      {
        *target = that.integers64_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerMatchingResource(OrthancPluginDatabaseTransaction* transaction,
                                                             OrthancPluginMatchingResource* target,
                                                             uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.matches_.size())
      {
        *target = that.matches_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerMetadata(OrthancPluginDatabaseTransaction* transaction,
                                                     int32_t* metadata,
                                                     const char** value,
                                                     uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.metadata_.size())
      {
        const Metadata& tmp = that.metadata_[index];
        *metadata = tmp.metadata;
        *value = tmp.value;
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadAnswerString(OrthancPluginDatabaseTransaction* transaction,
                                                   const char** target,
                                                   uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.stringAnswers_.size())
      {
        *target = that.stringAnswers_[index].c_str();
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    static OrthancPluginErrorCode ReadEventsCount(OrthancPluginDatabaseTransaction* transaction,
                                                  uint32_t* target /* out */)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);
      *target = static_cast<uint32_t>(that.events_.size());
      return OrthancPluginErrorCode_Success;
    }

    
    static OrthancPluginErrorCode ReadEvent(OrthancPluginDatabaseTransaction* transaction,
                                            OrthancPluginDatabaseEvent* event /* out */,
                                            uint32_t index)
    {
      const Output& that = *reinterpret_cast<const Output*>(transaction);

      if (index < that.events_.size())
      {
        *event = that.events_[index];
        return OrthancPluginErrorCode_Success;
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;
      }
    }

    
    virtual void SignalDeletedAttachment(const std::string& uuid,
                                         int32_t            contentType,
                                         uint64_t           uncompressedSize,
                                         const std::string& uncompressedHash,
                                         int32_t            compressionType,
                                         uint64_t           compressedSize,
                                         const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      OrthancPluginDatabaseEvent event;
      event.type = OrthancPluginDatabaseEventType_DeletedAttachment;
      event.content.attachment.uuid = StoreString(uuid);
      event.content.attachment.contentType = contentType;
      event.content.attachment.uncompressedSize = uncompressedSize;
      event.content.attachment.uncompressedHash = StoreString(uncompressedHash);
      event.content.attachment.compressionType = compressionType;
      event.content.attachment.compressedSize = compressedSize;
      event.content.attachment.compressedHash = StoreString(compressedHash);
        
      events_.push_back(event);
    }
    
    
    virtual void SignalDeletedResource(const std::string& publicId,
                                       OrthancPluginResourceType resourceType) ORTHANC_OVERRIDE
    {
      OrthancPluginDatabaseEvent event;
      event.type = OrthancPluginDatabaseEventType_DeletedResource;
      event.content.resource.level = resourceType;
      event.content.resource.publicId = StoreString(publicId);
        
      events_.push_back(event);
    }
    

    virtual void SignalRemainingAncestor(const std::string& ancestorId,
                                         OrthancPluginResourceType ancestorType) ORTHANC_OVERRIDE
    {
      OrthancPluginDatabaseEvent event;
      event.type = OrthancPluginDatabaseEventType_RemainingAncestor;
      event.content.resource.level = ancestorType;
      event.content.resource.publicId = StoreString(ancestorId);
        
      events_.push_back(event);
    }
    
    
    virtual void AnswerAttachment(const std::string& uuid,
                                  int32_t            contentType,
                                  uint64_t           uncompressedSize,
                                  const std::string& uncompressedHash,
                                  int32_t            compressionType,
                                  uint64_t           compressedSize,
                                  const std::string& compressedHash) ORTHANC_OVERRIDE
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Attachment);

      OrthancPluginAttachment attachment;
      attachment.uuid = StoreString(uuid);
      attachment.contentType = contentType;
      attachment.uncompressedSize = uncompressedSize;
      attachment.uncompressedHash = StoreString(uncompressedHash);
      attachment.compressionType = compressionType;
      attachment.compressedSize = compressedSize;
      attachment.compressedHash = StoreString(compressedHash);

      attachments_.push_back(attachment);
    }
    

    virtual void AnswerChange(int64_t                    seq,
                              int32_t                    changeType,
                              OrthancPluginResourceType  resourceType,
                              const std::string&         publicId,
                              const std::string&         date) ORTHANC_OVERRIDE
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Change);

      OrthancPluginChange change;
      change.seq = seq;
      change.changeType = changeType;
      change.resourceType = resourceType;
      change.publicId = StoreString(publicId);
      change.date = StoreString(date);

      changes_.push_back(change);
    }
    

    virtual void AnswerDicomTag(uint16_t group,
                                uint16_t element,
                                const std::string& value) ORTHANC_OVERRIDE
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_DicomTag);

      OrthancPluginDicomTag tag;
      tag.group = group;
      tag.element = element;
      tag.value = StoreString(value);

      tags_.push_back(tag);      
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
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_ExportedResource);

      OrthancPluginExportedResource exported;
      exported.seq = seq;
      exported.resourceType = resourceType;
      exported.publicId = StoreString(publicId);
      exported.modality = StoreString(modality);
      exported.date = StoreString(date);
      exported.patientId = StoreString(patientId);
      exported.studyInstanceUid = StoreString(studyInstanceUid);
      exported.seriesInstanceUid = StoreString(seriesInstanceUid);
      exported.sopInstanceUid = StoreString(sopInstanceUid);
  
      exported_.push_back(exported);
    }

    
    virtual void AnswerMatchingResource(const std::string& resourceId) ORTHANC_OVERRIDE
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_MatchingResource);

      OrthancPluginMatchingResource match;
      match.resourceId = StoreString(resourceId);
      match.someInstanceId = NULL;
        
      matches_.push_back(match);
    }
    
    
    virtual void AnswerMatchingResource(const std::string& resourceId,
                                        const std::string& someInstanceId) ORTHANC_OVERRIDE
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_MatchingResource);

      OrthancPluginMatchingResource match;
      match.resourceId = StoreString(resourceId);
      match.someInstanceId = StoreString(someInstanceId);
        
      matches_.push_back(match);
    }

    
    void AnswerIntegers32(const std::list<int32_t>& values)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Int32);

      integers32_.reserve(values.size());
      std::copy(std::begin(values), std::end(values), std::back_inserter(integers32_));
    }

    
    void AnswerIntegers64(const std::list<int64_t>& values)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Int64);

      integers64_.reserve(values.size());
      std::copy(std::begin(values), std::end(values), std::back_inserter(integers64_));
    }


    void AnswerMetadata(int32_t metadata,
                        const std::string& value)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Metadata);

      Metadata tmp;
      tmp.metadata = metadata;
      tmp.value = StoreString(value);

      metadata_.push_back(tmp);
    }
  };


  class Factory : public IDatabaseBackendOutput::IFactory
  {
  public:
    Factory()
    {
    }

    virtual IDatabaseBackendOutput* CreateOutput()
    {
      return new Output;
    }
  };


  class Transaction : public boost::noncopyable
  {
  private:
    IDatabaseBackend&        backend_;
    std::unique_ptr<Output>  output_;

  public:
    Transaction(IDatabaseBackend& backend) :
      backend_(backend),
      output_(new Output)
    {
    }

    IDatabaseBackend& GetBackend() const
    {
      return backend_;
    }

    Output& GetOutput() const
    {
      return *output_;
    }

    OrthancPluginContext* GetContext() const
    {
      return backend_.GetContext();
    }
  };

  
  static OrthancPluginErrorCode Open(void* database)
  {
    IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(database);

    try
    {
      backend->Open();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }

  
  static OrthancPluginErrorCode Close(void* database)
  {
    IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(database);

    try
    {
      backend->Close();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }

  
  static OrthancPluginErrorCode DestructDatabase(void* database)
  {
    // Nothing to delete, as this plugin uses a singleton to store backend
    if (database == NULL)
    {
      return OrthancPluginErrorCode_InternalError;
    }
    else
    {
      return OrthancPluginErrorCode_Success;
    }
  }

  
  static OrthancPluginErrorCode GetDatabaseVersion(void* database,
                                                   uint32_t* version)
  {
    IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(database);
      
    try
    {
      *version = backend->GetDatabaseVersion();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }


  static OrthancPluginErrorCode UpgradeDatabase(void* database,
                                                OrthancPluginStorageArea* storageArea,
                                                uint32_t  targetVersion)
  {
    IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(database);
      
    try
    {
      backend->UpgradeDatabase(targetVersion, storageArea);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }


  static OrthancPluginErrorCode StartTransaction(void* database,
                                                 OrthancPluginDatabaseTransaction** target /* out */,
                                                 OrthancPluginDatabaseTransactionType type)
  {
    IDatabaseBackend* backend = reinterpret_cast<IDatabaseBackend*>(database);
      
    try
    {
      std::unique_ptr<Transaction> transaction(new Transaction(*backend));
      
      switch (type)
      {
        case OrthancPluginDatabaseTransactionType_ReadOnly:
          backend->StartTransaction(TransactionType_ReadOnly);
          break;

        case OrthancPluginDatabaseTransactionType_ReadWrite:
          backend->StartTransaction(TransactionType_ReadWrite);
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
      }
      
      *target = reinterpret_cast<OrthancPluginDatabaseTransaction*>(transaction.release());
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }

  
  static OrthancPluginErrorCode DestructTransaction(OrthancPluginDatabaseTransaction* transaction)
  {
    if (transaction == NULL)
    {
      return OrthancPluginErrorCode_NullPointer;
    }
    else
    {
      delete reinterpret_cast<Output*>(transaction);
      return OrthancPluginErrorCode_Success;
    }
  }

  
  static OrthancPluginErrorCode Rollback(OrthancPluginDatabaseTransaction* transaction)
  {
    Transaction* t = reinterpret_cast<Transaction*>(transaction);

    try
    {
      t->GetBackend().RollbackTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode Commit(OrthancPluginDatabaseTransaction* transaction,
                                       int64_t fileSizeDelta /* TODO - not used? */)
  {
    Transaction* t = reinterpret_cast<Transaction*>(transaction);

    try
    {
      t->GetBackend().CommitTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode AddAttachment(OrthancPluginDatabaseTransaction* transaction,
                                              int64_t id,
                                              const OrthancPluginAttachment* attachment)
  {
    Transaction* t = reinterpret_cast<Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().AddAttachment(id, *attachment);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  

  static void RegisterV3(IDatabaseBackend& database)
  {
    OrthancPluginDatabaseBackendV3 params;
    memset(&params, 0, sizeof(params));

    params.readAnswersCount = Output::ReadAnswersCount;
    params.readAnswerAttachment = Output::ReadAnswerAttachment;
    params.readAnswerChange = Output::ReadAnswerChange;
    params.readAnswerDicomTag = Output::ReadAnswerDicomTag;
    params.readAnswerExportedResource = Output::ReadAnswerExportedResource;
    params.readAnswerInt32 = Output::ReadAnswerInt32;
    params.readAnswerInt64 = Output::ReadAnswerInt64;
    params.readAnswerMatchingResource = Output::ReadAnswerMatchingResource;
    params.readAnswerMetadata = Output::ReadAnswerMetadata;
    params.readAnswerString = Output::ReadAnswerString;
    
    params.readEventsCount = Output::ReadEventsCount;
    params.readEvent = Output::ReadEvent;

    params.open = Open;
    params.close = Close;
    params.destructDatabase = DestructDatabase;
    params.getDatabaseVersion = GetDatabaseVersion;
    params.upgradeDatabase = UpgradeDatabase;
    params.startTransaction = StartTransaction;
    params.destructTransaction = DestructTransaction;
    params.rollback = Rollback;
    params.commit = Commit;

    params.addAttachment = AddAttachment;

    OrthancPluginContext* context = database.GetContext();
 
    if (OrthancPluginRegisterDatabaseBackendV3(context, &params, sizeof(params), &database) != OrthancPluginErrorCode_Success)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError, "Unable to register the database backend");
    }

    database.SetOutputFactory(new Factory);
  }
}

#  endif
#endif



extern "C"
{
  ORTHANC_PLUGINS_API int32_t OrthancPluginInitialize(OrthancPluginContext* context)
  {
    if (!OrthancDatabases::InitializePlugin(context, "SQLite", true))
    {
      return -1;
    }

#if 0
    OrthancPlugins::OrthancConfiguration configuration;

    if (!configuration.IsSection("SQLite"))
    {
      LOG(WARNING) << "No available configuration for the SQLite index plugin";
      return 0;
    }

    OrthancPlugins::OrthancConfiguration sqlite;
    configuration.GetSection(sqlite, "SQLite");

    bool enable;
    if (!sqlite.LookupBooleanValue(enable, "EnableIndex") ||
        !enable)
    {
      LOG(WARNING) << "The SQLite index is currently disabled, set \"EnableIndex\" "
                   << "to \"true\" in the \"SQLite\" section of the configuration file of Orthanc";
      return 0;
    }
#endif

    try
    {
      /* Create the database back-end */
      backend_.reset(new OrthancDatabases::SQLiteIndex(context, "index.db"));  // TODO parameter

      /* Register the SQLite index into Orthanc */

      bool hasLoadedV3 = false;
      
#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 10, 0)
      if (OrthancPluginCheckVersionAdvanced(context, 1, 10, 0) == 1)
      {
        RegisterV3(*backend_);
        hasLoadedV3 = true;
      }
#  endif
#endif

      if (!hasLoadedV3)
      {
        OrthancDatabases::DatabaseBackendAdapterV2::Register(*backend_);
      }
    }
    catch (Orthanc::OrthancException& e)
    {
      LOG(ERROR) << e.What();
      return -1;
    }
    catch (...)
    {
      LOG(ERROR) << "Native exception while initializing the plugin";
      return -1;
    }

    return 0;
  }


  ORTHANC_PLUGINS_API void OrthancPluginFinalize()
  {
    LOG(WARNING) << "SQLite index is finalizing";
    backend_.reset(NULL);
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetName()
  {
    return "sqlite-index";
  }


  ORTHANC_PLUGINS_API const char* OrthancPluginGetVersion()
  {
    return ORTHANC_PLUGIN_VERSION;
  }
}
