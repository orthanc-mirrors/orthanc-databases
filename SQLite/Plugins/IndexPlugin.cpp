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

namespace OrthancDatabases
{
  class Output : public IDatabaseBackendOutput
  {
  private:
    _OrthancPluginDatabaseAnswerType            answerType_;
    std::list<std::string>                      strings_;
    
    std::vector<OrthancPluginAttachment>        attachments_;
    std::vector<OrthancPluginChange>            changes_;
    std::vector<OrthancPluginDicomTag>          tags_;
    std::vector<OrthancPluginExportedResource>  exported_;
    std::vector<OrthancPluginDatabaseEvent>     events_;
    
    const char* StoreString(const std::string& s)
    {
      strings_.push_back(s);
      return strings_.back().c_str();
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
      strings_.clear();
      
      attachments_.clear();
      changes_.clear();
      tags_.clear();
      exported_.clear();
      events_.clear();
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

    }
    
    
    virtual void AnswerMatchingResource(const std::string& resourceId,
                                        const std::string& someInstanceId) ORTHANC_OVERRIDE
    {

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

  
  static void Register()
  {
    OrthancPluginDatabaseBackendV3 backend;
    memset(&backend, 0, sizeof(backend));

    backend.readAnswersCount = Output::ReadAnswersCount;
    backend.readAnswerAttachment = Output::ReadAnswerAttachment;
    backend.readAnswerChange = Output::ReadAnswerChange;
    backend.readAnswerDicomTag = Output::ReadAnswerDicomTag;
    backend.readAnswerExportedResource = Output::ReadAnswerExportedResource;
    
    backend.readEventsCount = Output::ReadEventsCount;
    backend.readEvent = Output::ReadEvent;
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
      OrthancDatabases::DatabaseBackendAdapterV2::Register(context, *backend_);
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
