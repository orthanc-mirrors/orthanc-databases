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


#include "DatabaseBackendAdapterV3.h"

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 2)

#include <OrthancException.h>

#include <stdexcept>
#include <list>
#include <string>
#include <cassert>

#include <boost/thread/mutex.hpp>  // TODO - REMOVE


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
  class DatabaseBackendAdapterV3::Output : public IDatabaseBackendOutput
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
      // We don't systematically clear all the vectors, in order to
      // avoid spending unnecessary time
      
      switch (answerType_)
      {
        case _OrthancPluginDatabaseAnswerType_None:
          break;
        
        case _OrthancPluginDatabaseAnswerType_Attachment:
          attachments_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Change:
          changes_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_DicomTag:
          tags_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_ExportedResource:
          exported_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int32:
          integers32_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int64:
          integers64_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_MatchingResource:
          matches_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_Metadata:
          metadata_.clear();
          break;
        
        case _OrthancPluginDatabaseAnswerType_String:
          stringAnswers_.clear();
          break;
        
        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
      
      answerType_ = _OrthancPluginDatabaseAnswerType_None;
      stringsStore_.clear();
      events_.clear();
      
      assert(attachments_.empty());
      assert(changes_.empty());
      assert(tags_.empty());
      assert(exported_.empty());
      assert(events_.empty());
      assert(integers32_.empty());
      assert(integers64_.empty());
      assert(matches_.empty());
      assert(metadata_.empty());
      assert(stringAnswers_.empty());
    }


    OrthancPluginErrorCode ReadAnswersCount(uint32_t& target) const
    {
      switch (answerType_)
      {
        case _OrthancPluginDatabaseAnswerType_None:
          target = static_cast<uint32_t>(0);
          break;
          
        case _OrthancPluginDatabaseAnswerType_Attachment:
          target = static_cast<uint32_t>(attachments_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_Change:
          target = static_cast<uint32_t>(changes_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_DicomTag:
          target = static_cast<uint32_t>(tags_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_ExportedResource:
          target = static_cast<uint32_t>(exported_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int32:
          target = static_cast<uint32_t>(integers32_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_Int64:
          target = static_cast<uint32_t>(integers64_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_MatchingResource:
          target = static_cast<uint32_t>(matches_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_Metadata:
          target = static_cast<uint32_t>(metadata_.size());
          break;
        
        case _OrthancPluginDatabaseAnswerType_String:
          target = static_cast<uint32_t>(stringAnswers_.size());
          break;
        
        default:
          return OrthancPluginErrorCode_InternalError;
      }

      return OrthancPluginErrorCode_Success;
    }


    OrthancPluginErrorCode ReadAnswerAttachment(OrthancPluginAttachment& target /* out */,
                                                uint32_t index) const
    {
      if (index < attachments_.size())
      {
        target = attachments_[index];
        return OrthancPluginErrorCode_Success;
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;
      }
    }


    OrthancPluginErrorCode ReadAnswerChange(OrthancPluginChange& target /* out */,
                                            uint32_t index) const
    {
      if (index < changes_.size())
      {
        target = changes_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerDicomTag(uint16_t& group,
                                              uint16_t& element,
                                              const char*& value,
                                              uint32_t index) const
    {
      if (index < tags_.size())
      {
        const OrthancPluginDicomTag& tag = tags_[index];
        group = tag.group;
        element = tag.element;
        value = tag.value;
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerExportedResource(OrthancPluginExportedResource& target /* out */,
                                                      uint32_t index) const
    {
      if (index < exported_.size())
      {
        target = exported_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerInt32(int32_t& target,
                                           uint32_t index) const
    {
      if (index < integers32_.size())
      {
        target = integers32_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerInt64(int64_t& target,
                                           uint32_t index) const
    {
      if (index < integers64_.size())
      {
        target = integers64_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerMatchingResource(OrthancPluginMatchingResource& target,
                                                      uint32_t index) const
    {
      if (index < matches_.size())
      {
        target = matches_[index];
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerMetadata(int32_t& metadata,
                                              const char*& value,
                                              uint32_t index) const
    {
      if (index < metadata_.size())
      {
        const Metadata& tmp = metadata_[index];
        metadata = tmp.metadata;
        value = tmp.value;
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadAnswerString(const char*& target,
                                            uint32_t index) const
    {
      if (index < stringAnswers_.size())
      {
        target = stringAnswers_[index].c_str();
        return OrthancPluginErrorCode_Success;        
      }
      else
      {
        return OrthancPluginErrorCode_ParameterOutOfRange;        
      }
    }


    OrthancPluginErrorCode ReadEventsCount(uint32_t& target /* out */) const
    {
      target = static_cast<uint32_t>(events_.size());
      return OrthancPluginErrorCode_Success;
    }

    
    OrthancPluginErrorCode ReadEvent(OrthancPluginDatabaseEvent& event /* out */,
                                     uint32_t index) const
    {
      if (index < events_.size())
      {
        event = events_[index];
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


    void AnswerInteger64(int64_t value)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_Int64);

      integers64_.resize(1);
      integers64_[0] = value;
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


    void AnswerStrings(const std::list<std::string>& values)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_String);

      stringAnswers_.reserve(values.size());
      std::copy(std::begin(values), std::end(values), std::back_inserter(stringAnswers_));
    }


    void AnswerString(const std::string& value)
    {
      SetupAnswerType(_OrthancPluginDatabaseAnswerType_String);

      if (stringAnswers_.empty())
      {
        stringAnswers_.push_back(value);
      }
      else
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
      }
    }
  };


  IDatabaseBackendOutput* DatabaseBackendAdapterV3::Factory::CreateOutput()
  {
    return new DatabaseBackendAdapterV3::Output;
  }


  class DatabaseBackendAdapterV3::Transaction : public boost::noncopyable
  {
  private:
    boost::mutex::scoped_lock  lock_;    // TODO - REMOVE
    IndexBackend&          backend_;
    std::unique_ptr<Output>    output_;

    static boost::mutex& GetMutex()   // TODO - REMOVE
    {
      static boost::mutex mutex_;
      return mutex_;
    }
    
  public:
    Transaction(IndexBackend& backend) :
      lock_(GetMutex()),
      backend_(backend),
      output_(new Output)
    {
    }

    ~Transaction()
    {
    }

    IndexBackend& GetBackend() const
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

  
  static OrthancPluginErrorCode ReadAnswersCount(OrthancPluginDatabaseTransaction* transaction,
                                                 uint32_t* target /* out */)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswersCount(*target);
  }


  static OrthancPluginErrorCode ReadAnswerAttachment(OrthancPluginDatabaseTransaction* transaction,
                                                     OrthancPluginAttachment* target /* out */,
                                                     uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerAttachment(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerChange(OrthancPluginDatabaseTransaction* transaction,
                                                 OrthancPluginChange* target /* out */,
                                                 uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerChange(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerDicomTag(OrthancPluginDatabaseTransaction* transaction,
                                                   uint16_t* group,
                                                   uint16_t* element,
                                                   const char** value,
                                                   uint32_t index)
  {
    assert(group != NULL);
    assert(element != NULL);
    assert(value != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerDicomTag(*group, *element, *value, index);
  }


  static OrthancPluginErrorCode ReadAnswerExportedResource(OrthancPluginDatabaseTransaction* transaction,
                                                           OrthancPluginExportedResource* target /* out */,
                                                           uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerExportedResource(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerInt32(OrthancPluginDatabaseTransaction* transaction,
                                                int32_t* target,
                                                uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerInt32(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerInt64(OrthancPluginDatabaseTransaction* transaction,
                                                int64_t* target,
                                                uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerInt64(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerMatchingResource(OrthancPluginDatabaseTransaction* transaction,
                                                           OrthancPluginMatchingResource* target,
                                                           uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerMatchingResource(*target, index);
  }


  static OrthancPluginErrorCode ReadAnswerMetadata(OrthancPluginDatabaseTransaction* transaction,
                                                   int32_t* metadata,
                                                   const char** value,
                                                   uint32_t index)
  {
    assert(metadata != NULL);
    assert(value != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerMetadata(*metadata, *value, index);
  }


  static OrthancPluginErrorCode ReadAnswerString(OrthancPluginDatabaseTransaction* transaction,
                                                 const char** target,
                                                 uint32_t index)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadAnswerString(*target, index);
  }


  static OrthancPluginErrorCode ReadEventsCount(OrthancPluginDatabaseTransaction* transaction,
                                                uint32_t* target /* out */)
  {
    assert(target != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadEventsCount(*target);
  }

    
  static OrthancPluginErrorCode ReadEvent(OrthancPluginDatabaseTransaction* transaction,
                                          OrthancPluginDatabaseEvent* event /* out */,
                                          uint32_t index)
  {
    assert(event != NULL);
    const DatabaseBackendAdapterV3::Transaction& that = *reinterpret_cast<const DatabaseBackendAdapterV3::Transaction*>(transaction);
    return that.GetOutput().ReadEvent(*event, index);
  }

    
  static OrthancPluginErrorCode Open(void* database)
  {
    IndexBackend* backend = reinterpret_cast<IndexBackend*>(database);

    try
    {
      backend->Open();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(backend->GetContext());
  }

  
  static OrthancPluginErrorCode Close(void* database)
  {
    IndexBackend* backend = reinterpret_cast<IndexBackend*>(database);

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
    IndexBackend* backend = reinterpret_cast<IndexBackend*>(database);
      
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
    IndexBackend* backend = reinterpret_cast<IndexBackend*>(database);
      
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
    IndexBackend* backend = reinterpret_cast<IndexBackend*>(database);
      
    try
    {
      std::unique_ptr<DatabaseBackendAdapterV3::Transaction> transaction(new DatabaseBackendAdapterV3::Transaction(*backend));
      
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
      delete reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);
      return OrthancPluginErrorCode_Success;
    }
  }

  
  static OrthancPluginErrorCode Rollback(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().RollbackTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode Commit(OrthancPluginDatabaseTransaction* transaction,
                                       int64_t fileSizeDelta /* TODO - not used? */)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().CommitTransaction();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode AddAttachment(OrthancPluginDatabaseTransaction* transaction,
                                              int64_t id,
                                              const OrthancPluginAttachment* attachment)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().AddAttachment(id, *attachment);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode ClearChanges(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().ClearChanges();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode ClearExportedResources(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().ClearExportedResources();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode ClearMainDicomTags(OrthancPluginDatabaseTransaction* transaction,
                                                   int64_t resourceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().ClearMainDicomTags(resourceId);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode CreateInstance(OrthancPluginDatabaseTransaction* transaction,
                                               OrthancPluginCreateInstanceResult* target /* out */,
                                               const char* hashPatient,
                                               const char* hashStudy,
                                               const char* hashSeries,
                                               const char* hashInstance)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      if (t->GetBackend().HasCreateInstance())
      {
        t->GetBackend().CreateInstance(*target, hashPatient, hashStudy, hashSeries, hashInstance);
      }
      else
      {
        t->GetBackend().CreateInstanceGeneric(*target, hashPatient, hashStudy, hashSeries, hashInstance);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode DeleteAttachment(OrthancPluginDatabaseTransaction* transaction,
                                                 int64_t id,
                                                 int32_t contentType)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().DeleteAttachment(t->GetOutput(), id, contentType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode DeleteMetadata(OrthancPluginDatabaseTransaction* transaction,
                                               int64_t id,
                                               int32_t metadataType)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().DeleteMetadata(id, metadataType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode DeleteResource(OrthancPluginDatabaseTransaction* transaction,
                                               int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().DeleteResource(t->GetOutput(), id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetAllMetadata(OrthancPluginDatabaseTransaction* transaction,
                                               int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::map<int32_t, std::string> values;
      t->GetBackend().GetAllMetadata(values, id);

      for (std::map<int32_t, std::string>::const_iterator it = values.begin(); it != values.end(); ++it)
      {
        t->GetOutput().AnswerMetadata(it->first, it->second);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetAllPublicIds(OrthancPluginDatabaseTransaction* transaction,
                                                OrthancPluginResourceType resourceType)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<std::string> values;
      t->GetBackend().GetAllPublicIds(values, resourceType);
      t->GetOutput().AnswerStrings(values);
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetAllPublicIdsWithLimit(OrthancPluginDatabaseTransaction* transaction,
                                                         OrthancPluginResourceType resourceType,
                                                         uint64_t since,
                                                         uint64_t limit)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<std::string> values;
      t->GetBackend().GetAllPublicIds(values, resourceType, since, limit);
      t->GetOutput().AnswerStrings(values);
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetChanges(OrthancPluginDatabaseTransaction* transaction,
                                           uint8_t* targetDone /* out */,
                                           int64_t since,
                                           uint32_t maxResults)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      bool done;
      t->GetBackend().GetChanges(t->GetOutput(), done, since, maxResults);
      *targetDone = (done ? 1 : 0);
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetChildrenInternalId(OrthancPluginDatabaseTransaction* transaction,
                                                      int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<int64_t> values;
      t->GetBackend().GetChildrenInternalId(values, id);
      t->GetOutput().AnswerIntegers64(values);
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetChildrenMetadata(OrthancPluginDatabaseTransaction* transaction,
                                                    int64_t resourceId,
                                                    int32_t metadata)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<std::string> values;
      t->GetBackend().GetChildrenMetadata(values, resourceId, metadata);
      t->GetOutput().AnswerStrings(values);
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetChildrenPublicId(OrthancPluginDatabaseTransaction* transaction,
                                                    int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<std::string> values;
      t->GetBackend().GetChildrenPublicId(values, id);
      t->GetOutput().AnswerStrings(values);
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetExportedResources(OrthancPluginDatabaseTransaction* transaction,
                                                     uint8_t* targetDone /* out */,
                                                     int64_t since,
                                                     uint32_t maxResults)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      bool done;
      t->GetBackend().GetExportedResources(t->GetOutput(), done, since, maxResults);
      *targetDone = (done ? 1 : 0);
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetLastChange(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().GetLastChange(t->GetOutput());
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetLastChangeIndex(OrthancPluginDatabaseTransaction* transaction,
                                                   int64_t* target)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      *target = t->GetBackend().GetLastChangeIndex();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetLastExportedResource(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().GetLastExportedResource(t->GetOutput());
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetMainDicomTags(OrthancPluginDatabaseTransaction* transaction,
                                                 int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().GetMainDicomTags(t->GetOutput(), id);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode GetPublicId(OrthancPluginDatabaseTransaction* transaction,
                                            int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetOutput().AnswerString(t->GetBackend().GetPublicId(id));
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode GetResourcesCount(OrthancPluginDatabaseTransaction* transaction,
                                                  uint64_t* target /* out */,
                                                  OrthancPluginResourceType resourceType)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      *target = t->GetBackend().GetResourcesCount(resourceType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode GetResourceType(OrthancPluginDatabaseTransaction* transaction,
                                                OrthancPluginResourceType* target /* out */,
                                                uint64_t resourceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      *target = t->GetBackend().GetResourceType(resourceId);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode GetTotalCompressedSize(OrthancPluginDatabaseTransaction* transaction,
                                                       uint64_t* target /* out */)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      *target = t->GetBackend().GetTotalCompressedSize();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode GetTotalUncompressedSize(OrthancPluginDatabaseTransaction* transaction,
                                                         uint64_t* target /* out */)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      *target = t->GetBackend().GetTotalUncompressedSize();
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode IsDiskSizeAbove(OrthancPluginDatabaseTransaction* transaction,
                                                uint8_t* target,
                                                uint64_t threshold)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      bool above = (t->GetBackend().GetTotalCompressedSize() >= threshold);
      *target = (above ? 1 : 0);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode IsExistingResource(OrthancPluginDatabaseTransaction* transaction,
                                                   uint8_t* target,
                                                   int64_t resourceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      bool exists = t->GetBackend().IsExistingResource(resourceId);
      *target = (exists ? 1 : 0);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode IsProtectedPatient(OrthancPluginDatabaseTransaction* transaction,
                                                   uint8_t* target,
                                                   int64_t resourceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      bool isProtected = t->GetBackend().IsProtectedPatient(resourceId);
      *target = (isProtected ? 1 : 0);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode ListAvailableAttachments(OrthancPluginDatabaseTransaction* transaction,
                                                         int64_t resourceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::list<int32_t> values;
      t->GetBackend().ListAvailableAttachments(values, resourceId);
      t->GetOutput().AnswerIntegers32(values);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode LogChange(OrthancPluginDatabaseTransaction* transaction,
                                          int32_t changeType,
                                          int64_t resourceId,
                                          OrthancPluginResourceType resourceType,
                                          const char* date)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().LogChange(changeType, resourceId, resourceType, date);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }
  

  static OrthancPluginErrorCode LogExportedResource(OrthancPluginDatabaseTransaction* transaction,
                                                    OrthancPluginResourceType resourceType,
                                                    const char* publicId,
                                                    const char* modality,
                                                    const char* date,
                                                    const char* patientId,
                                                    const char* studyInstanceUid,
                                                    const char* seriesInstanceUid,
                                                    const char* sopInstanceUid)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      OrthancPluginExportedResource exported;
      exported.seq = 0;
      exported.resourceType = resourceType;
      exported.publicId = publicId;
      exported.modality = modality;
      exported.date = date;
      exported.patientId = patientId;
      exported.studyInstanceUid = studyInstanceUid;
      exported.seriesInstanceUid = seriesInstanceUid;
      exported.sopInstanceUid = sopInstanceUid;
        
      t->GetOutput().Clear();
      t->GetBackend().LogExportedResource(exported);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupAttachment(OrthancPluginDatabaseTransaction* transaction,
                                                 int64_t resourceId,
                                                 int32_t contentType)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().LookupAttachment(t->GetOutput(), resourceId, contentType);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupGlobalProperty(OrthancPluginDatabaseTransaction* transaction,
                                                     const char* serverIdentifier,
                                                     int32_t property)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::string s;
      if (t->GetBackend().LookupGlobalProperty(s, serverIdentifier, property))
      {
        t->GetOutput().AnswerString(s);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupMetadata(OrthancPluginDatabaseTransaction* transaction,
                                               int64_t id,
                                               int32_t metadata)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::string s;
      if (t->GetBackend().LookupMetadata(s, id, metadata))
      {
        t->GetOutput().AnswerString(s);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupParent(OrthancPluginDatabaseTransaction* transaction,
                                             int64_t id)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      int64_t parentId;
      if (t->GetBackend().LookupParent(parentId, id))
      {
        t->GetOutput().AnswerInteger64(parentId);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupResource(OrthancPluginDatabaseTransaction* transaction,
                                               uint8_t* isExisting /* out */,
                                               int64_t* id /* out */,
                                               OrthancPluginResourceType* type /* out */,
                                               const char* publicId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      if (t->GetBackend().LookupResource(*id, *type, publicId))
      {
        *isExisting = 1;
      }
      else
      {
        *isExisting = 0;
      }
        
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupResources(OrthancPluginDatabaseTransaction* transaction,
                                                uint32_t constraintsCount,
                                                const OrthancPluginDatabaseConstraint* constraints,
                                                OrthancPluginResourceType queryLevel,
                                                uint32_t limit,
                                                uint8_t requestSomeInstanceId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::vector<Orthanc::DatabaseConstraint> lookup;
      lookup.reserve(constraintsCount);

      for (uint32_t i = 0; i < constraintsCount; i++)
      {
        lookup.push_back(Orthanc::DatabaseConstraint(constraints[i]));
      }
        
      t->GetBackend().LookupResources(t->GetOutput(), lookup, queryLevel, limit, (requestSomeInstanceId != 0));
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }


  static OrthancPluginErrorCode LookupResourceAndParent(OrthancPluginDatabaseTransaction* transaction,
                                                        uint8_t* isExisting /* out */,
                                                        int64_t* id /* out */,
                                                        OrthancPluginResourceType* type /* out */,
                                                        const char* publicId)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();

      std::string parent;
      if (t->GetBackend().LookupResourceAndParent(*id, *type, parent, publicId))
      {
        *isExisting = 1;

        if (!parent.empty())
        {
          t->GetOutput().AnswerString(parent);
        }
      }
      else
      {
        *isExisting = 0;
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

  
  static OrthancPluginErrorCode SelectPatientToRecycle(OrthancPluginDatabaseTransaction* transaction)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      
      int64_t id;
      if (t->GetBackend().SelectPatientToRecycle(id))
      {
        t->GetOutput().AnswerInteger64(id);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  static OrthancPluginErrorCode SelectPatientToRecycle2(OrthancPluginDatabaseTransaction* transaction,
                                                        int64_t patientIdToAvoid)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      
      int64_t id;
      if (t->GetBackend().SelectPatientToRecycle(id, patientIdToAvoid))
      {
        t->GetOutput().AnswerInteger64(id);
      }
      
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  static OrthancPluginErrorCode SetGlobalProperty(OrthancPluginDatabaseTransaction* transaction,
                                                  const char* serverIdentifier,
                                                  int32_t property,
                                                  const char* value)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().SetGlobalProperty(serverIdentifier, property, value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  static OrthancPluginErrorCode SetMetadata(OrthancPluginDatabaseTransaction* transaction,
                                            int64_t id,
                                            int32_t metadata,
                                            const char* value)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().SetMetadata(id, metadata, value);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  static OrthancPluginErrorCode SetProtectedPatient(OrthancPluginDatabaseTransaction* transaction,
                                                    int64_t id,
                                                    uint8_t isProtected)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().SetProtectedPatient(id, (isProtected != 0));
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  static OrthancPluginErrorCode SetResourcesContent(OrthancPluginDatabaseTransaction* transaction,
                                                    uint32_t countIdentifierTags,
                                                    const OrthancPluginResourcesContentTags* identifierTags,
                                                    uint32_t countMainDicomTags,
                                                    const OrthancPluginResourcesContentTags* mainDicomTags,
                                                    uint32_t countMetadata,
                                                    const OrthancPluginResourcesContentMetadata* metadata)
  {
    DatabaseBackendAdapterV3::Transaction* t = reinterpret_cast<DatabaseBackendAdapterV3::Transaction*>(transaction);

    try
    {
      t->GetOutput().Clear();
      t->GetBackend().SetResourcesContent(countIdentifierTags, identifierTags,
                                          countMainDicomTags, mainDicomTags,
                                          countMetadata, metadata);
      return OrthancPluginErrorCode_Success;
    }
    ORTHANC_PLUGINS_DATABASE_CATCH(t->GetContext());
  }

    
  void DatabaseBackendAdapterV3::Register(IndexBackend& database)
  {
    OrthancPluginDatabaseBackendV3 params;
    memset(&params, 0, sizeof(params));

    params.readAnswersCount = ReadAnswersCount;
    params.readAnswerAttachment = ReadAnswerAttachment;
    params.readAnswerChange = ReadAnswerChange;
    params.readAnswerDicomTag = ReadAnswerDicomTag;
    params.readAnswerExportedResource = ReadAnswerExportedResource;
    params.readAnswerInt32 = ReadAnswerInt32;
    params.readAnswerInt64 = ReadAnswerInt64;
    params.readAnswerMatchingResource = ReadAnswerMatchingResource;
    params.readAnswerMetadata = ReadAnswerMetadata;
    params.readAnswerString = ReadAnswerString;
    
    params.readEventsCount = ReadEventsCount;
    params.readEvent = ReadEvent;

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
    params.clearChanges = ClearChanges;
    params.clearExportedResources = ClearExportedResources;
    params.clearMainDicomTags = ClearMainDicomTags;
    params.createInstance = CreateInstance;
    params.deleteAttachment = DeleteAttachment;
    params.deleteMetadata = DeleteMetadata;
    params.deleteResource = DeleteResource;
    params.getAllMetadata = GetAllMetadata;
    params.getAllPublicIds = GetAllPublicIds;
    params.getAllPublicIdsWithLimit = GetAllPublicIdsWithLimit;
    params.getChanges = GetChanges;
    params.getChildrenInternalId = GetChildrenInternalId;
    params.getChildrenMetadata = GetChildrenMetadata;
    params.getChildrenPublicId = GetChildrenPublicId;
    params.getExportedResources = GetExportedResources;
    params.getLastChange = GetLastChange;
    params.getLastChangeIndex = GetLastChangeIndex;
    params.getLastExportedResource = GetLastExportedResource;
    params.getMainDicomTags = GetMainDicomTags;
    params.getPublicId = GetPublicId;
    params.getResourcesCount = GetResourcesCount;
    params.getResourceType = GetResourceType;
    params.getTotalCompressedSize = GetTotalCompressedSize;
    params.getTotalUncompressedSize = GetTotalUncompressedSize;
    params.isDiskSizeAbove = IsDiskSizeAbove;
    params.isExistingResource = IsExistingResource;
    params.isProtectedPatient = IsProtectedPatient;
    params.listAvailableAttachments = ListAvailableAttachments;
    params.logChange = LogChange;
    params.logExportedResource = LogExportedResource;
    params.lookupAttachment = LookupAttachment;
    params.lookupGlobalProperty = LookupGlobalProperty;
    params.lookupMetadata = LookupMetadata;
    params.lookupParent = LookupParent;
    params.lookupResource = LookupResource;
    params.lookupResources = LookupResources;
    params.lookupResourceAndParent = LookupResourceAndParent;
    params.selectPatientToRecycle = SelectPatientToRecycle;
    params.selectPatientToRecycle2 = SelectPatientToRecycle2;
    params.setGlobalProperty = SetGlobalProperty;
    params.setMetadata = SetMetadata;
    params.setProtectedPatient = SetProtectedPatient;
    params.setResourcesContent = SetResourcesContent;

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
