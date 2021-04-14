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


#include "IndexBackend.h"

#include "../../Resources/Orthanc/Databases/ISqlLookupFormatter.h"
#include "../Common/BinaryStringValue.h"
#include "../Common/Integer64Value.h"
#include "../Common/Utf8StringValue.h"
#include "DatabaseBackendAdapterV2.h"
#include "DatabaseBackendAdapterV3.h"
#include "GlobalProperties.h"

#include <Compatibility.h>  // For std::unique_ptr<>
#include <Logging.h>
#include <OrthancException.h>


namespace OrthancDatabases
{
  static std::string ConvertWildcardToLike(const std::string& query)
  {
    std::string s = query;

    for (size_t i = 0; i < s.size(); i++)
    {
      if (s[i] == '*')
      {
        s[i] = '%';
      }
      else if (s[i] == '?')
      {
        s[i] = '_';
      }
    }

    // TODO Escape underscores and percents

    return s;
  }

  
  int64_t IndexBackend::ReadInteger64(const DatabaseManager::StatementBase& statement,
                                      size_t field)
  {
    if (statement.IsDone())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
    }

    const IValue& value = statement.GetResultField(field);
      
    switch (value.GetType())
    {
      case ValueType_Integer64:
        return dynamic_cast<const Integer64Value&>(value).GetValue();

      default:
        //LOG(ERROR) << value.Format();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }


  int32_t IndexBackend::ReadInteger32(const DatabaseManager::StatementBase& statement,
                                      size_t field)
  {
    if (statement.IsDone())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
    }

    int64_t value = ReadInteger64(statement, field);

    if (value != static_cast<int64_t>(static_cast<int32_t>(value)))
    {
      LOG(ERROR) << "Integer overflow";
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
    else
    {
      return static_cast<int32_t>(value);
    }
  }

    
  std::string IndexBackend::ReadString(const DatabaseManager::StatementBase& statement,
                                       size_t field)
  {
    const IValue& value = statement.GetResultField(field);

    switch (value.GetType())
    {
      case ValueType_BinaryString:
        return dynamic_cast<const BinaryStringValue&>(value).GetContent();

      case ValueType_Utf8String:
        return dynamic_cast<const Utf8StringValue&>(value).GetContent();

      default:
        //LOG(ERROR) << value.Format();
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }

    
  template <typename T>
  void IndexBackend::ReadListOfIntegers(std::list<T>& target,
                                        DatabaseManager::CachedStatement& statement,
                                        const Dictionary& args)
  {
    statement.Execute(args);
      
    target.clear();

    if (!statement.IsDone())
    {
      if (statement.GetResultFieldsCount() != 1)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
      
      statement.SetResultFieldType(0, ValueType_Integer64);

      while (!statement.IsDone())
      {
        target.push_back(static_cast<T>(ReadInteger64(statement, 0)));
        statement.Next();
      }
    }
  }

    
  void IndexBackend::ReadListOfStrings(std::list<std::string>& target,
                                       DatabaseManager::CachedStatement& statement,
                                       const Dictionary& args)
  {
    statement.Execute(args);

    target.clear();
      
    if (!statement.IsDone())
    {
      if (statement.GetResultFieldsCount() != 1)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
      
      while (!statement.IsDone())
      {
        target.push_back(ReadString(statement, 0));
        statement.Next();
      }
    }
  }


  void IndexBackend::ReadChangesInternal(IDatabaseBackendOutput& output,
                                         bool& done,
                                         DatabaseManager& manager,
                                         DatabaseManager::CachedStatement& statement,
                                         const Dictionary& args,
                                         uint32_t maxResults)
  {
    statement.Execute(args);

    uint32_t count = 0;

    while (count < maxResults &&
           !statement.IsDone())
    {
      output.AnswerChange(
        ReadInteger64(statement, 0),
        ReadInteger32(statement, 1),
        static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 3)),
        GetPublicId(manager, ReadInteger64(statement, 2)),
        ReadString(statement, 4));

      statement.Next();
      count++;
    }

    done = (count < maxResults ||
            statement.IsDone());
  }


  void IndexBackend::ReadExportedResourcesInternal(IDatabaseBackendOutput& output,
                                                   bool& done,
                                                   DatabaseManager::CachedStatement& statement,
                                                   const Dictionary& args,
                                                   uint32_t maxResults)
  {
    statement.Execute(args);

    uint32_t count = 0;

    while (count < maxResults &&
           !statement.IsDone())
    {
      int64_t seq = ReadInteger64(statement, 0);
      OrthancPluginResourceType resourceType =
        static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 1));
      std::string publicId = ReadString(statement, 2);

      output.AnswerExportedResource(seq, 
                                    resourceType,
                                    publicId,
                                    ReadString(statement, 3),  // modality
                                    ReadString(statement, 8),  // date
                                    ReadString(statement, 4),  // patient ID
                                    ReadString(statement, 5),  // study instance UID
                                    ReadString(statement, 6),  // series instance UID
                                    ReadString(statement, 7)); // sop instance UID
      
      statement.Next();
      count++;
    }

    done = (count < maxResults ||
            statement.IsDone());
  }


  void IndexBackend::ClearDeletedFiles(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "DELETE FROM DeletedFiles");

    statement.Execute();
  }
    

  void IndexBackend::ClearDeletedResources(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "DELETE FROM DeletedResources");

    statement.Execute();
  }
    

  void IndexBackend::SignalDeletedFiles(IDatabaseBackendOutput& output,
                                        DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM DeletedFiles");

    statement.SetReadOnly(true);
    statement.Execute();

    while (!statement.IsDone())
    {
      std::string a = ReadString(statement, 0);
      std::string b = ReadString(statement, 5);
      std::string c = ReadString(statement, 6);

      output.SignalDeletedAttachment(a.c_str(),
                                     ReadInteger32(statement, 1),
                                     ReadInteger64(statement, 3),
                                     b.c_str(),
                                     ReadInteger32(statement, 4),
                                     ReadInteger64(statement, 2),
                                     c.c_str());
      
      statement.Next();
    }
  }


  void IndexBackend::SignalDeletedResources(IDatabaseBackendOutput& output,
                                            DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM DeletedResources");

    statement.SetReadOnly(true);
    statement.Execute();

    while (!statement.IsDone())
    {
      output.SignalDeletedResource(
        ReadString(statement, 1),
        static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 0)));

      statement.Next();
    }
  }


  IndexBackend::IndexBackend(OrthancPluginContext* context) :
    context_(context)
  {
  }


  void IndexBackend::SetOutputFactory(IDatabaseBackendOutput::IFactory* factory)
  {
    boost::unique_lock<boost::shared_mutex> lock(outputFactoryMutex_);
      
    if (factory == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else if (outputFactory_.get() != NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      outputFactory_.reset(factory);
    }
  }


  IDatabaseBackendOutput* IndexBackend::CreateOutput()
  {
    boost::shared_lock<boost::shared_mutex> lock(outputFactoryMutex_);
      
    if (outputFactory_.get() == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_BadSequenceOfCalls);
    }
    else
    {
      return outputFactory_->CreateOutput();
    }
  }

  
  void IndexBackend::AddAttachment(DatabaseManager& manager,
                                   int64_t id,
                                   const OrthancPluginAttachment& attachment)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO AttachedFiles VALUES(${id}, ${type}, ${uuid}, "
      "${compressed}, ${uncompressed}, ${compression}, ${hash}, ${hash-compressed})");

    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("type", ValueType_Integer64);
    statement.SetParameterType("uuid", ValueType_Utf8String);
    statement.SetParameterType("compressed", ValueType_Integer64);
    statement.SetParameterType("uncompressed", ValueType_Integer64);
    statement.SetParameterType("compression", ValueType_Integer64);
    statement.SetParameterType("hash", ValueType_Utf8String);
    statement.SetParameterType("hash-compressed", ValueType_Utf8String);

    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("type", attachment.contentType);
    args.SetUtf8Value("uuid", attachment.uuid);
    args.SetIntegerValue("compressed", attachment.compressedSize);
    args.SetIntegerValue("uncompressed", attachment.uncompressedSize);
    args.SetIntegerValue("compression", attachment.compressionType);
    args.SetUtf8Value("hash", attachment.uncompressedHash);
    args.SetUtf8Value("hash-compressed", attachment.compressedHash);
    
    statement.Execute(args);
  }

    
  void IndexBackend::AttachChild(DatabaseManager& manager,
                                 int64_t parent,
                                 int64_t child)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "UPDATE Resources SET parentId = ${parent} WHERE internalId = ${child}");

    statement.SetParameterType("parent", ValueType_Integer64);
    statement.SetParameterType("child", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("parent", parent);
    args.SetIntegerValue("child", child);
    
    statement.Execute(args);
  }

    
  void IndexBackend::ClearChanges(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "DELETE FROM Changes");

    statement.Execute();
  }

    
  void IndexBackend::ClearExportedResources(DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "DELETE FROM ExportedResources");

    statement.Execute();
  }

    
  void IndexBackend::DeleteAttachment(IDatabaseBackendOutput& output,
                                      DatabaseManager& manager,
                                      int64_t id,
                                      int32_t attachment)
  {
    ClearDeletedFiles(manager);

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM AttachedFiles WHERE id=${id} AND fileType=${type}");

      statement.SetParameterType("id", ValueType_Integer64);
      statement.SetParameterType("type", ValueType_Integer64);

      Dictionary args;
      args.SetIntegerValue("id", id);
      args.SetIntegerValue("type", static_cast<int>(attachment));
    
      statement.Execute(args);
    }

    SignalDeletedFiles(output, manager);
  }

    
  void IndexBackend::DeleteMetadata(DatabaseManager& manager,
                                    int64_t id,
                                    int32_t metadataType)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "DELETE FROM Metadata WHERE id=${id} and type=${type}");

    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("type", static_cast<int>(metadataType));
    
    statement.Execute(args);
  }

    
  void IndexBackend::DeleteResource(IDatabaseBackendOutput& output,
                                    DatabaseManager& manager,
                                    int64_t id)
  {
    assert(manager.GetDialect() != Dialect_MySQL);
    
    ClearDeletedFiles(manager);
    ClearDeletedResources(manager);
    
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM RemainingAncestor");

      statement.Execute();
    }
      
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM Resources WHERE internalId=${id}");

      statement.SetParameterType("id", ValueType_Integer64);

      Dictionary args;
      args.SetIntegerValue("id", id);
    
      statement.Execute(args);
    }


    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT * FROM RemainingAncestor");

      statement.Execute();

      if (!statement.IsDone())
      {
        output.SignalRemainingAncestor(
          ReadString(statement, 1),
          static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 0)));
          
        // There is at most 1 remaining ancestor
        assert((statement.Next(), statement.IsDone()));
      }
    }
    
    SignalDeletedFiles(output, manager);
    SignalDeletedResources(output, manager);
  }


  void IndexBackend::GetAllInternalIds(std::list<int64_t>& target,
                                       DatabaseManager& manager,
                                       OrthancPluginResourceType resourceType)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT internalId FROM Resources WHERE resourceType=${type}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("type", static_cast<int>(resourceType));

    ReadListOfIntegers<int64_t>(target, statement, args);
  }

    
  void IndexBackend::GetAllPublicIds(std::list<std::string>& target,
                                     DatabaseManager& manager,
                                     OrthancPluginResourceType resourceType)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT publicId FROM Resources WHERE resourceType=${type}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("type", static_cast<int>(resourceType));

    ReadListOfStrings(target, statement, args);
  }

    
  void IndexBackend::GetAllPublicIds(std::list<std::string>& target,
                                     DatabaseManager& manager,
                                     OrthancPluginResourceType resourceType,
                                     uint64_t since,
                                     uint64_t limit)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT publicId FROM (SELECT publicId FROM Resources "
      "WHERE resourceType=${type}) AS tmp "
      "ORDER BY tmp.publicId LIMIT ${limit} OFFSET ${since}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("type", ValueType_Integer64);
    statement.SetParameterType("limit", ValueType_Integer64);
    statement.SetParameterType("since", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("type", static_cast<int>(resourceType));
    args.SetIntegerValue("limit", limit);
    args.SetIntegerValue("since", since);

    ReadListOfStrings(target, statement, args);
  }

    
  /* Use GetOutput().AnswerChange() */
  void IndexBackend::GetChanges(IDatabaseBackendOutput& output,
                                bool& done /*out*/,
                                DatabaseManager& manager,
                                int64_t since,
                                uint32_t maxResults)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM Changes WHERE seq>${since} ORDER BY seq LIMIT ${limit}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("limit", ValueType_Integer64);
    statement.SetParameterType("since", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("limit", maxResults + 1);
    args.SetIntegerValue("since", since);

    ReadChangesInternal(output, done, manager, statement, args, maxResults);
  }

    
  void IndexBackend::GetChildrenInternalId(std::list<int64_t>& target /*out*/,
                                           DatabaseManager& manager,
                                           int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT a.internalId FROM Resources AS a, Resources AS b  "
      "WHERE a.parentId = b.internalId AND b.internalId = ${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    ReadListOfIntegers<int64_t>(target, statement, args);
  }

    
  void IndexBackend::GetChildrenPublicId(std::list<std::string>& target /*out*/,
                                         DatabaseManager& manager,
                                         int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT a.publicId FROM Resources AS a, Resources AS b  "
      "WHERE a.parentId = b.internalId AND b.internalId = ${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    ReadListOfStrings(target, statement, args);
  }

    
  /* Use GetOutput().AnswerExportedResource() */
  void IndexBackend::GetExportedResources(IDatabaseBackendOutput& output,
                                          bool& done /*out*/,
                                          DatabaseManager& manager,
                                          int64_t since,
                                          uint32_t maxResults)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM ExportedResources WHERE seq>${since} ORDER BY seq LIMIT ${limit}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("limit", ValueType_Integer64);
    statement.SetParameterType("since", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("limit", maxResults + 1);
    args.SetIntegerValue("since", since);

    ReadExportedResourcesInternal(output, done, statement, args, maxResults);
  }

    
  /* Use GetOutput().AnswerChange() */
  void IndexBackend::GetLastChange(IDatabaseBackendOutput& output,
                                   DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM Changes ORDER BY seq DESC LIMIT 1");

    statement.SetReadOnly(true);
      
    Dictionary args;

    bool done;  // Ignored
    ReadChangesInternal(output, done, manager, statement, args, 1);
  }

    
  /* Use GetOutput().AnswerExportedResource() */
  void IndexBackend::GetLastExportedResource(IDatabaseBackendOutput& output,
                                             DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM ExportedResources ORDER BY seq DESC LIMIT 1");

    statement.SetReadOnly(true);
      
    Dictionary args;

    bool done;  // Ignored
    ReadExportedResourcesInternal(output, done, statement, args, 1);
  }

    
  /* Use GetOutput().AnswerDicomTag() */
  void IndexBackend::GetMainDicomTags(IDatabaseBackendOutput& output,
                                      DatabaseManager& manager,
                                      int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM MainDicomTags WHERE id=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    statement.Execute(args);

    while (!statement.IsDone())
    {
      output.AnswerDicomTag(static_cast<uint16_t>(ReadInteger64(statement, 1)),
                            static_cast<uint16_t>(ReadInteger64(statement, 2)),
                            ReadString(statement, 3));
      statement.Next();
    }
  }

    
  std::string IndexBackend::GetPublicId(DatabaseManager& manager,
                                        int64_t resourceId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT publicId FROM Resources WHERE internalId=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", resourceId);

    statement.Execute(args);

    if (statement.IsDone())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_UnknownResource);
    }
    else
    {
      return ReadString(statement, 0);
    }
  }

    
  uint64_t IndexBackend::GetResourcesCount(DatabaseManager& manager,
                                           OrthancPluginResourceType resourceType)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    switch (manager.GetDialect())
    {
      case Dialect_MySQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS UNSIGNED INT) FROM Resources WHERE resourceType=${type}"));
        break;

      case Dialect_PostgreSQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS BIGINT) FROM Resources WHERE resourceType=${type}"));
        break;

      case Dialect_SQLite:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT COUNT(*) FROM Resources WHERE resourceType=${type}"));
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }

    statement->SetReadOnly(true);
    statement->SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("type", resourceType);

    statement->Execute(args);

    return static_cast<uint64_t>(ReadInteger64(*statement, 0));
  }

    
  OrthancPluginResourceType IndexBackend::GetResourceType(DatabaseManager& manager,
                                                          int64_t resourceId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT resourceType FROM Resources WHERE internalId=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", resourceId);

    statement.Execute(args);

    if (statement.IsDone())
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_UnknownResource);
    }
    else
    {
      return static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 0));
    }
  }

    
  uint64_t IndexBackend::GetTotalCompressedSize(DatabaseManager& manager)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    // NB: "COALESCE" is used to replace "NULL" by "0" if the number of rows is empty

    switch (manager.GetDialect())
    {
      case Dialect_MySQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COALESCE(SUM(compressedSize), 0) AS UNSIGNED INTEGER) FROM AttachedFiles"));
        break;
        
      case Dialect_PostgreSQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COALESCE(SUM(compressedSize), 0) AS BIGINT) FROM AttachedFiles"));
        break;

      case Dialect_SQLite:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT COALESCE(SUM(compressedSize), 0) FROM AttachedFiles"));
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }

    statement->SetReadOnly(true);
    statement->Execute();

    return static_cast<uint64_t>(ReadInteger64(*statement, 0));
  }

    
  uint64_t IndexBackend::GetTotalUncompressedSize(DatabaseManager& manager)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    // NB: "COALESCE" is used to replace "NULL" by "0" if the number of rows is empty

    switch (manager.GetDialect())
    {
      case Dialect_MySQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COALESCE(SUM(uncompressedSize), 0) AS UNSIGNED INTEGER) FROM AttachedFiles"));
        break;
        
      case Dialect_PostgreSQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COALESCE(SUM(uncompressedSize), 0) AS BIGINT) FROM AttachedFiles"));
        break;

      case Dialect_SQLite:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT COALESCE(SUM(uncompressedSize), 0) FROM AttachedFiles"));
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }

    statement->SetReadOnly(true);
    statement->Execute();

    return static_cast<uint64_t>(ReadInteger64(*statement, 0));
  }

    
  bool IndexBackend::IsExistingResource(DatabaseManager& manager,
                                        int64_t internalId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM Resources WHERE internalId=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", internalId);

    statement.Execute(args);

    return !statement.IsDone();
  }

    
  bool IndexBackend::IsProtectedPatient(DatabaseManager& manager,
                                        int64_t internalId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT * FROM PatientRecyclingOrder WHERE patientId = ${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", internalId);

    statement.Execute(args);

    return statement.IsDone();
  }

    
  void IndexBackend::ListAvailableMetadata(std::list<int32_t>& target /*out*/,
                                           DatabaseManager& manager,
                                           int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT type FROM Metadata WHERE id=${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    ReadListOfIntegers<int32_t>(target, statement, args);
  }

    
  void IndexBackend::ListAvailableAttachments(std::list<int32_t>& target /*out*/,
                                              DatabaseManager& manager,
                                              int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT fileType FROM AttachedFiles WHERE id=${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    ReadListOfIntegers<int32_t>(target, statement, args);
  }

    
  void IndexBackend::LogChange(DatabaseManager& manager,
                               int32_t changeType,
                               int64_t resourceId,
                               OrthancPluginResourceType resourceType,
                               const char* date)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO Changes VALUES(${}, ${changeType}, ${id}, ${resourceType}, ${date})");

    statement.SetParameterType("changeType", ValueType_Integer64);
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("resourceType", ValueType_Integer64);
    statement.SetParameterType("date", ValueType_Utf8String);

    Dictionary args;
    args.SetIntegerValue("changeType", changeType);
    args.SetIntegerValue("id", resourceId);
    args.SetIntegerValue("resourceType", resourceType);
    args.SetUtf8Value("date", date);

    statement.Execute(args);
  }

    
  void IndexBackend::LogExportedResource(DatabaseManager& manager,
                                         const OrthancPluginExportedResource& resource)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO ExportedResources VALUES(${}, ${type}, ${publicId}, "
      "${modality}, ${patient}, ${study}, ${series}, ${instance}, ${date})");

    statement.SetParameterType("type", ValueType_Integer64);
    statement.SetParameterType("publicId", ValueType_Utf8String);
    statement.SetParameterType("modality", ValueType_Utf8String);
    statement.SetParameterType("patient", ValueType_Utf8String);
    statement.SetParameterType("study", ValueType_Utf8String);
    statement.SetParameterType("series", ValueType_Utf8String);
    statement.SetParameterType("instance", ValueType_Utf8String);
    statement.SetParameterType("date", ValueType_Utf8String);

    Dictionary args;
    args.SetIntegerValue("type", resource.resourceType);
    args.SetUtf8Value("publicId", resource.publicId);
    args.SetUtf8Value("modality", resource.modality);
    args.SetUtf8Value("patient", resource.patientId);
    args.SetUtf8Value("study", resource.studyInstanceUid);
    args.SetUtf8Value("series", resource.seriesInstanceUid);
    args.SetUtf8Value("instance", resource.sopInstanceUid);
    args.SetUtf8Value("date", resource.date);

    statement.Execute(args);
  }

    
  /* Use GetOutput().AnswerAttachment() */
  bool IndexBackend::LookupAttachment(IDatabaseBackendOutput& output,
                                      DatabaseManager& manager,
                                      int64_t id,
                                      int32_t contentType)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT uuid, uncompressedSize, compressionType, compressedSize, "
      "uncompressedHash, compressedHash FROM AttachedFiles WHERE id=${id} AND fileType=${type}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("type", static_cast<int>(contentType));

    statement.Execute(args);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      output.AnswerAttachment(ReadString(statement, 0),
                              contentType,
                              ReadInteger64(statement, 1),
                              ReadString(statement, 4),
                              ReadInteger32(statement, 2),
                              ReadInteger64(statement, 3),
                              ReadString(statement, 5));
      return true;
    }
  }


  static bool ReadGlobalProperty(std::string& target,
                                 DatabaseManager::CachedStatement& statement,
                                 const Dictionary& args)
  {
    statement.Execute(args);
    statement.SetResultFieldType(0, ValueType_Utf8String);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      ValueType type = statement.GetResultField(0).GetType();

      if (type == ValueType_Null)
      {
        return false;
      }
      else if (type != ValueType_Utf8String)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
      }
      else
      {
        target = dynamic_cast<const Utf8StringValue&>(statement.GetResultField(0)).GetContent();
        return true;
      }
    }
  }
  
    
  bool IndexBackend::LookupGlobalProperty(std::string& target /*out*/,
                                          DatabaseManager& manager,
                                          const char* serverIdentifier,
                                          int32_t property)
  {
    if (serverIdentifier == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else
    {
      if (strlen(serverIdentifier) == 0)
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "SELECT value FROM GlobalProperties WHERE property=${property}");

        statement.SetReadOnly(true);
        statement.SetParameterType("property", ValueType_Integer64);

        Dictionary args;
        args.SetIntegerValue("property", property);

        return ReadGlobalProperty(target, statement, args);
      }
      else
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "SELECT value FROM ServerProperties WHERE server=${server} AND property=${property}");

        statement.SetReadOnly(true);
        statement.SetParameterType("server", ValueType_Utf8String);
        statement.SetParameterType("property", ValueType_Integer64);

        Dictionary args;
        args.SetUtf8Value("server", serverIdentifier);
        args.SetIntegerValue("property", property);

        return ReadGlobalProperty(target, statement, args);
      }
    }
  }

    
  void IndexBackend::LookupIdentifier(std::list<int64_t>& target /*out*/,
                                      DatabaseManager& manager,
                                      OrthancPluginResourceType resourceType,
                                      uint16_t group,
                                      uint16_t element,
                                      OrthancPluginIdentifierConstraint constraint,
                                      const char* value)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    std::string header =
      "SELECT d.id FROM DicomIdentifiers AS d, Resources AS r WHERE "
      "d.id = r.internalId AND r.resourceType=${type} AND d.tagGroup=${group} "
      "AND d.tagElement=${element} AND ";
      
    switch (constraint)
    {
      case OrthancPluginIdentifierConstraint_Equal:
        header += "d.value = ${value}";
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager, header.c_str()));
        break;
        
      case OrthancPluginIdentifierConstraint_SmallerOrEqual:
        header += "d.value <= ${value}";
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager, header.c_str()));
        break;
        
      case OrthancPluginIdentifierConstraint_GreaterOrEqual:
        header += "d.value >= ${value}";
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager, header.c_str()));
        break;
        
      case OrthancPluginIdentifierConstraint_Wildcard:
        header += "d.value LIKE ${value}";
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager, header.c_str()));
        break;
        
      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
    }

    statement->SetReadOnly(true);
    statement->SetParameterType("type", ValueType_Integer64);
    statement->SetParameterType("group", ValueType_Integer64);
    statement->SetParameterType("element", ValueType_Integer64);
    statement->SetParameterType("value", ValueType_Utf8String);

    Dictionary args;
    args.SetIntegerValue("type", resourceType);
    args.SetIntegerValue("group", group);
    args.SetIntegerValue("element", element);

    if (constraint == OrthancPluginIdentifierConstraint_Wildcard)
    {
      args.SetUtf8Value("value", ConvertWildcardToLike(value));
    }
    else
    {
      args.SetUtf8Value("value", value);
    }

    statement->Execute(args);

    target.clear();
    while (!statement->IsDone())
    {
      target.push_back(ReadInteger64(*statement, 0));
      statement->Next();
    }
  }

    
  void IndexBackend::LookupIdentifierRange(std::list<int64_t>& target /*out*/,
                                           DatabaseManager& manager,
                                           OrthancPluginResourceType resourceType,
                                           uint16_t group,
                                           uint16_t element,
                                           const char* start,
                                           const char* end)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT d.id FROM DicomIdentifiers AS d, Resources AS r WHERE "
      "d.id = r.internalId AND r.resourceType=${type} AND d.tagGroup=${group} "
      "AND d.tagElement=${element} AND d.value>=${start} AND d.value<=${end}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("type", ValueType_Integer64);
    statement.SetParameterType("group", ValueType_Integer64);
    statement.SetParameterType("element", ValueType_Integer64);
    statement.SetParameterType("start", ValueType_Utf8String);
    statement.SetParameterType("end", ValueType_Utf8String);
    
    Dictionary args;
    args.SetIntegerValue("type", resourceType);
    args.SetIntegerValue("group", group);
    args.SetIntegerValue("element", element);
    args.SetUtf8Value("start", start);
    args.SetUtf8Value("end", end);

    statement.Execute(args);

    target.clear();
    while (!statement.IsDone())
    {
      target.push_back(ReadInteger64(statement, 0));
      statement.Next();
    }
  }

    
  bool IndexBackend::LookupMetadata(std::string& target /*out*/,
                                    DatabaseManager& manager,
                                    int64_t id,
                                    int32_t metadataType)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT value FROM Metadata WHERE id=${id} and type=${type}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("type", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("type", metadataType);

    statement.Execute(args);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      target = ReadString(statement, 0);
      return true;
    }
  } 

    
  bool IndexBackend::LookupParent(int64_t& parentId /*out*/,
                                  DatabaseManager& manager,
                                  int64_t resourceId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT parentId FROM Resources WHERE internalId=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", resourceId);

    statement.Execute(args);

    if (statement.IsDone() ||
        statement.GetResultField(0).GetType() == ValueType_Null)
    {
      return false;
    }
    else
    {
      parentId = ReadInteger64(statement, 0);
      return true;
    }
  }

    
  bool IndexBackend::LookupResource(int64_t& id /*out*/,
                                    OrthancPluginResourceType& type /*out*/,
                                    DatabaseManager& manager,
                                    const char* publicId)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT internalId, resourceType FROM Resources WHERE publicId=${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Utf8String);

    Dictionary args;
    args.SetUtf8Value("id", publicId);

    statement.Execute(args);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      id = ReadInteger64(statement, 0);
      type = static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 1));
      return true;
    }
  }

    
  bool IndexBackend::SelectPatientToRecycle(int64_t& internalId /*out*/,
                                            DatabaseManager& manager)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT patientId FROM PatientRecyclingOrder ORDER BY seq ASC LIMIT 1");

    statement.SetReadOnly(true);
    statement.Execute();

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      internalId = ReadInteger64(statement, 0);
      return true;
    }
  }

    
  bool IndexBackend::SelectPatientToRecycle(int64_t& internalId /*out*/,
                                            DatabaseManager& manager,
                                            int64_t patientIdToAvoid)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT patientId FROM PatientRecyclingOrder "
      "WHERE patientId != ${id} ORDER BY seq ASC LIMIT 1");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", patientIdToAvoid);

    statement.Execute(args);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      internalId = ReadInteger64(statement, 0);
      return true;
    }
  }


  static void RunSetGlobalPropertyStatement(DatabaseManager::CachedStatement& statement,
                                            bool hasServer,
                                            bool hasValue,
                                            const char* serverIdentifier,
                                            int32_t property,
                                            const char* utf8)
  {
    Dictionary args;

    statement.SetParameterType("property", ValueType_Integer64);
    args.SetIntegerValue("property", static_cast<int>(property));

    if (hasValue)
    {
      assert(utf8 != NULL);
      statement.SetParameterType("value", ValueType_Utf8String);
      args.SetUtf8Value("value", utf8);
    }
    else
    {
      assert(utf8 == NULL);
    }        

    if (hasServer)
    {
      assert(serverIdentifier != NULL && strlen(serverIdentifier) > 0);
      statement.SetParameterType("server", ValueType_Utf8String);
      args.SetUtf8Value("server", serverIdentifier);
    }
    else
    {
      assert(serverIdentifier == NULL);
    }
      
    statement.Execute(args);
  }

    
  void IndexBackend::SetGlobalProperty(DatabaseManager& manager,
                                       const char* serverIdentifier,
                                       int32_t property,
                                       const char* utf8)
  {
    if (serverIdentifier == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    else if (manager.GetDialect() == Dialect_SQLite)
    {
      bool hasServer = (strlen(serverIdentifier) != 0);

      if (hasServer)
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT OR REPLACE INTO ServerProperties VALUES (${server}, ${property}, ${value})");

        RunSetGlobalPropertyStatement(statement, true, true, serverIdentifier, property, utf8);
      }
      else
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT OR REPLACE INTO GlobalProperties VALUES (${property}, ${value})");

        RunSetGlobalPropertyStatement(statement, false, true, NULL, property, utf8);
      }
    }
    else
    {
      bool hasServer = (strlen(serverIdentifier) != 0);

      if (hasServer)
      {
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager,
            "DELETE FROM ServerProperties WHERE server=${server} AND property=${property}");

          RunSetGlobalPropertyStatement(statement, true, false, serverIdentifier, property, NULL);
        }

        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager,
            "INSERT INTO ServerProperties VALUES (${server}, ${property}, ${value})");

          RunSetGlobalPropertyStatement(statement, true, true, serverIdentifier, property, utf8);
        }
      }
      else
      {
        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager,
            "DELETE FROM GlobalProperties WHERE property=${property}");
      
          RunSetGlobalPropertyStatement(statement, false, false, NULL, property, NULL);
        }

        {
          DatabaseManager::CachedStatement statement(
            STATEMENT_FROM_HERE, manager,
            "INSERT INTO GlobalProperties VALUES (${property}, ${value})");
      
          RunSetGlobalPropertyStatement(statement, false, true, NULL, property, utf8);
        }
      }
    }
  }


  static void ExecuteSetTag(DatabaseManager::CachedStatement& statement,
                            int64_t id,
                            uint16_t group,
                            uint16_t element,
                            const char* value)
  {
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("group", ValueType_Integer64);
    statement.SetParameterType("element", ValueType_Integer64);
    statement.SetParameterType("value", ValueType_Utf8String);
        
    Dictionary args;
    args.SetIntegerValue("id", id);
    args.SetIntegerValue("group", group);
    args.SetIntegerValue("element", element);
    args.SetUtf8Value("value", value);
        
    statement.Execute(args);
  }

    
  void IndexBackend::SetMainDicomTag(DatabaseManager& manager,
                                     int64_t id,
                                     uint16_t group,
                                     uint16_t element,
                                     const char* value)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO MainDicomTags VALUES(${id}, ${group}, ${element}, ${value})");

    ExecuteSetTag(statement, id, group, element, value);
  }

    
  void IndexBackend::SetIdentifierTag(DatabaseManager& manager,
                                      int64_t id,
                                      uint16_t group,
                                      uint16_t element,
                                      const char* value)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "INSERT INTO DicomIdentifiers VALUES(${id}, ${group}, ${element}, ${value})");
        
    ExecuteSetTag(statement, id, group, element, value);
  } 

    
  void IndexBackend::SetMetadata(DatabaseManager& manager,
                                 int64_t id,
                                 int32_t metadataType,
                                 const char* value)
  {
    if (manager.GetDialect() == Dialect_SQLite)
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "INSERT OR REPLACE INTO Metadata VALUES (${id}, ${type}, ${value})");
        
      statement.SetParameterType("id", ValueType_Integer64);
      statement.SetParameterType("type", ValueType_Integer64);
      statement.SetParameterType("value", ValueType_Utf8String);
        
      Dictionary args;
      args.SetIntegerValue("id", id);
      args.SetIntegerValue("type", metadataType);
      args.SetUtf8Value("value", value);
        
      statement.Execute(args);
    }
    else
    {
      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "DELETE FROM Metadata WHERE id=${id} AND type=${type}");
        
        statement.SetParameterType("id", ValueType_Integer64);
        statement.SetParameterType("type", ValueType_Integer64);
        
        Dictionary args;
        args.SetIntegerValue("id", id);
        args.SetIntegerValue("type", metadataType);
        
        statement.Execute(args);
      }

      {
        DatabaseManager::CachedStatement statement(
          STATEMENT_FROM_HERE, manager,
          "INSERT INTO Metadata VALUES (${id}, ${type}, ${value})");
        
        statement.SetParameterType("id", ValueType_Integer64);
        statement.SetParameterType("type", ValueType_Integer64);
        statement.SetParameterType("value", ValueType_Utf8String);
        
        Dictionary args;
        args.SetIntegerValue("id", id);
        args.SetIntegerValue("type", metadataType);
        args.SetUtf8Value("value", value);
        
        statement.Execute(args);
      }
    }
  }

    
  void IndexBackend::SetProtectedPatient(DatabaseManager& manager,
                                         int64_t internalId, 
                                         bool isProtected)
  {
    if (isProtected)
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM PatientRecyclingOrder WHERE patientId=${id}");
        
      statement.SetParameterType("id", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("id", internalId);
        
      statement.Execute(args);
    }
    else if (IsProtectedPatient(manager, internalId))
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "INSERT INTO PatientRecyclingOrder VALUES(${}, ${id})");
        
      statement.SetParameterType("id", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("id", internalId);
        
      statement.Execute(args);
    }
    else
    {
      // Nothing to do: The patient is already unprotected
    }
  }

    
  uint32_t IndexBackend::GetDatabaseVersion(DatabaseManager& manager)
  {
    // Create a read-only, explicit transaction to read the database
    // version (this was a read-write, implicit transaction in
    // PostgreSQL plugin <= 3.3 and MySQL plugin <= 3.0)
    DatabaseManager::Transaction transaction(manager, TransactionType_ReadOnly);
    
    std::string version = "unknown";
      
    if (LookupGlobalProperty(version, manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseSchemaVersion))
    {
      try
      {
        return boost::lexical_cast<unsigned int>(version);
      }
      catch (boost::bad_lexical_cast&)
      {
      }
    }

    LOG(ERROR) << "The database is corrupted. Drop it manually for Orthanc to recreate it";
    throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
  }

    
  /**
   * Upgrade the database to the specified version of the database
   * schema.  The upgrade script is allowed to make calls to
   * OrthancPluginReconstructMainDicomTags().
   **/
  void IndexBackend::UpgradeDatabase(DatabaseManager& manager,
                                     uint32_t  targetVersion,
                                     OrthancPluginStorageArea* storageArea)
  {
    LOG(ERROR) << "Upgrading database is not implemented by this plugin";
    throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
  }

    
  void IndexBackend::ClearMainDicomTags(DatabaseManager& manager,
                                        int64_t internalId)
  {
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM MainDicomTags WHERE id=${id}");
        
      statement.SetParameterType("id", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("id", internalId);
        
      statement.Execute(args);
    }

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM DicomIdentifiers WHERE id=${id}");
        
      statement.SetParameterType("id", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("id", internalId);
        
      statement.Execute(args);
    }
  }


  // For unit testing only!
  uint64_t IndexBackend::GetAllResourcesCount(DatabaseManager& manager)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    switch (manager.GetDialect())
    {
      case Dialect_MySQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS UNSIGNED INT) FROM Resources"));
        break;
        
      case Dialect_PostgreSQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS BIGINT) FROM Resources"));
        break;

      case Dialect_SQLite:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT COUNT(*) FROM Resources"));
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }

    statement->SetReadOnly(true);
    statement->Execute();

    return static_cast<uint64_t>(ReadInteger64(*statement, 0));
  }    


  // For unit testing only!
  uint64_t IndexBackend::GetUnprotectedPatientsCount(DatabaseManager& manager)
  {
    std::unique_ptr<DatabaseManager::CachedStatement> statement;

    switch (manager.GetDialect())
    {
      case Dialect_MySQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS UNSIGNED INT) FROM PatientRecyclingOrder"));
        break;
        
      case Dialect_PostgreSQL:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT CAST(COUNT(*) AS BIGINT) FROM PatientRecyclingOrder"));
        break;

      case Dialect_SQLite:
        statement.reset(new DatabaseManager::CachedStatement(
                          STATEMENT_FROM_HERE, manager,
                          "SELECT COUNT(*) FROM PatientRecyclingOrder"));
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
    }

    statement->SetReadOnly(true);
    statement->Execute();

    return static_cast<uint64_t>(ReadInteger64(*statement, 0));
  }    


  // For unit testing only!
  bool IndexBackend::GetParentPublicId(std::string& target,
                                       DatabaseManager& manager,
                                       int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT a.publicId FROM Resources AS a, Resources AS b "
      "WHERE a.internalId = b.parentId AND b.internalId = ${id}");

    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    statement.Execute(args);

    if (statement.IsDone())
    {
      return false;
    }
    else
    {
      target = ReadString(statement, 0);
      return true;
    }
  }


  // For unit tests only!
  void IndexBackend::GetChildren(std::list<std::string>& childrenPublicIds,
                                 DatabaseManager& manager,
                                 int64_t id)
  { 
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT publicId FROM Resources WHERE parentId=${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    ReadListOfStrings(childrenPublicIds, statement, args);
  }


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  class IndexBackend::LookupFormatter : public Orthanc::ISqlLookupFormatter
  {
  private:
    Dialect     dialect_;
    size_t      count_;
    Dictionary  dictionary_;

    static std::string FormatParameter(size_t index)
    {
      return "p" + boost::lexical_cast<std::string>(index);
    }
    
  public:
    LookupFormatter(Dialect dialect) :
      dialect_(dialect),
      count_(0)
    {
    }

    virtual std::string GenerateParameter(const std::string& value)
    {
      const std::string key = FormatParameter(count_);

      count_ ++;
      dictionary_.SetUtf8Value(key, value);

      return "${" + key + "}";
    }

    virtual std::string FormatResourceType(Orthanc::ResourceType level)
    {
      return boost::lexical_cast<std::string>(Orthanc::Plugins::Convert(level));
    }

    virtual std::string FormatWildcardEscape()
    {
      switch (dialect_)
      {
        case Dialect_SQLite:
        case Dialect_PostgreSQL:
          return "ESCAPE '\\'";

        case Dialect_MySQL:
          return "ESCAPE '\\\\'";

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
      }
    }

    void PrepareStatement(DatabaseManager::StandaloneStatement& statement) const
    {
      statement.SetReadOnly(true);
      
      for (size_t i = 0; i < count_; i++)
      {
        statement.SetParameterType(FormatParameter(i), ValueType_Utf8String);
      }
    }

    const Dictionary& GetDictionary() const
    {
      return dictionary_;
    }
  };
#endif

  
#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  // New primitive since Orthanc 1.5.2
  void IndexBackend::LookupResources(IDatabaseBackendOutput& output,
                                     DatabaseManager& manager,
                                     const std::vector<Orthanc::DatabaseConstraint>& lookup,
                                     OrthancPluginResourceType queryLevel,
                                     uint32_t limit,
                                     bool requestSomeInstance)
  {
    LookupFormatter formatter(manager.GetDialect());

    std::string sql;
    Orthanc::ISqlLookupFormatter::Apply(sql, formatter, lookup,
                                        Orthanc::Plugins::Convert(queryLevel), limit);

    if (requestSomeInstance)
    {
      // Composite query to find some instance if requested
      switch (queryLevel)
      {
        case OrthancPluginResourceType_Patient:
          sql = ("SELECT patients.publicId, MIN(instances.publicId) FROM (" + sql + ") patients "
                 "INNER JOIN Resources studies   ON studies.parentId   = patients.internalId "
                 "INNER JOIN Resources series    ON series.parentId    = studies.internalId "
                 "INNER JOIN Resources instances ON instances.parentId = series.internalId "
                 "GROUP BY patients.publicId");
          break;

        case OrthancPluginResourceType_Study:
          sql = ("SELECT studies.publicId, MIN(instances.publicId) FROM (" + sql + ") studies "
                 "INNER JOIN Resources series    ON series.parentId    = studies.internalId "
                 "INNER JOIN Resources instances ON instances.parentId = series.internalId "
                 "GROUP BY studies.publicId");                 
          break;

        case OrthancPluginResourceType_Series:
          sql = ("SELECT series.publicId, MIN(instances.publicId) FROM (" + sql + ") series "
                 "INNER JOIN Resources instances ON instances.parentId = series.internalId "
                 "GROUP BY series.publicId");
          break;

        case OrthancPluginResourceType_Instance:
          sql = ("SELECT instances.publicId, instances.publicId FROM (" + sql + ") instances");
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
    }

    DatabaseManager::StandaloneStatement statement(manager, sql);
    formatter.PrepareStatement(statement);

    statement.Execute(formatter.GetDictionary());

    while (!statement.IsDone())
    {
      if (requestSomeInstance)
      {
        output.AnswerMatchingResource(ReadString(statement, 0), ReadString(statement, 1));
      }
      else
      {
        output.AnswerMatchingResource(ReadString(statement, 0));
      }

      statement.Next();
    }    
  }
#endif


#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static void ExecuteSetResourcesContentTags(
    DatabaseManager& manager,
    const std::string& table,
    const std::string& variablePrefix,
    uint32_t count,
    const OrthancPluginResourcesContentTags* tags)
  {
    std::string sql;
    Dictionary args;
    
    for (uint32_t i = 0; i < count; i++)
    {
      std::string name = variablePrefix + boost::lexical_cast<std::string>(i);

      args.SetUtf8Value(name, tags[i].value);
      
      std::string insert = ("(" + boost::lexical_cast<std::string>(tags[i].resource) + ", " +
                            boost::lexical_cast<std::string>(tags[i].group) + ", " +
                            boost::lexical_cast<std::string>(tags[i].element) + ", " +
                            "${" + name + "})");

      if (sql.empty())
      {
        sql = "INSERT INTO " + table + " VALUES " + insert;
      }
      else
      {
        sql += ", " + insert;
      }
    }

    if (!sql.empty())
    {
      DatabaseManager::StandaloneStatement statement(manager, sql);

      for (uint32_t i = 0; i < count; i++)
      {
        statement.SetParameterType(variablePrefix + boost::lexical_cast<std::string>(i),
                                   ValueType_Utf8String);
      }

      statement.Execute(args);
    }
  }
#endif
  

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  static void ExecuteSetResourcesContentMetadata(
    DatabaseManager& manager,
    uint32_t count,
    const OrthancPluginResourcesContentMetadata* metadata)
  {
    std::string sqlRemove;  // To overwrite    
    std::string sqlInsert;
    Dictionary args;
    
    for (uint32_t i = 0; i < count; i++)
    {
      std::string name = "m" + boost::lexical_cast<std::string>(i);

      args.SetUtf8Value(name, metadata[i].value);
      
      std::string insert = ("(" + boost::lexical_cast<std::string>(metadata[i].resource) + ", " +
                            boost::lexical_cast<std::string>(metadata[i].metadata) + ", " +
                            "${" + name + "})");

      std::string remove = ("(id=" + boost::lexical_cast<std::string>(metadata[i].resource) +
                            " AND type=" + boost::lexical_cast<std::string>(metadata[i].metadata)
                            + ")");

      if (sqlInsert.empty())
      {
        sqlInsert = "INSERT INTO Metadata VALUES " + insert;
      }
      else
      {
        sqlInsert += ", " + insert;
      }

      if (sqlRemove.empty())
      {
        sqlRemove = "DELETE FROM Metadata WHERE " + remove;
      }
      else
      {
        sqlRemove += " OR " + remove;
      }
    }

    if (!sqlRemove.empty())
    {
      DatabaseManager::StandaloneStatement statement(manager, sqlRemove);
      statement.Execute();
    }
    
    if (!sqlInsert.empty())
    {
      DatabaseManager::StandaloneStatement statement(manager, sqlInsert);

      for (uint32_t i = 0; i < count; i++)
      {
        statement.SetParameterType("m" + boost::lexical_cast<std::string>(i),
                                   ValueType_Utf8String);
      }

      statement.Execute(args);
    }
  }
#endif
  

#if ORTHANC_PLUGINS_HAS_DATABASE_CONSTRAINT == 1
  // New primitive since Orthanc 1.5.2
  void IndexBackend::SetResourcesContent(
    DatabaseManager& manager,
    uint32_t countIdentifierTags,
    const OrthancPluginResourcesContentTags* identifierTags,
    uint32_t countMainDicomTags,
    const OrthancPluginResourcesContentTags* mainDicomTags,
    uint32_t countMetadata,
    const OrthancPluginResourcesContentMetadata* metadata)
  {
    /**
     * TODO - PostgreSQL doesn't allow multiple commands in a prepared
     * statement, so we execute 3 separate commands (for identifiers,
     * main tags and metadata). Maybe MySQL does not suffer from the
     * same limitation, to check.
     **/
    
    ExecuteSetResourcesContentTags(manager, "DicomIdentifiers", "i",
                                   countIdentifierTags, identifierTags);

    ExecuteSetResourcesContentTags(manager, "MainDicomTags", "t",
                                   countMainDicomTags, mainDicomTags);

    ExecuteSetResourcesContentMetadata(manager, countMetadata, metadata);
  }
#endif


  // New primitive since Orthanc 1.5.2
  void IndexBackend::GetChildrenMetadata(std::list<std::string>& target,
                                         DatabaseManager& manager,
                                         int64_t resourceId,
                                         int32_t metadata)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT value FROM Metadata WHERE type=${metadata} AND "
      "id IN (SELECT internalId FROM Resources WHERE parentId=${id})");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);
    statement.SetParameterType("metadata", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", static_cast<int>(resourceId));
    args.SetIntegerValue("metadata", static_cast<int>(metadata));

    ReadListOfStrings(target, statement, args);
  }


  // New primitive since Orthanc 1.5.2
  void IndexBackend::TagMostRecentPatient(DatabaseManager& manager,
                                          int64_t patient)
  {
    int64_t seq;
    
    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "SELECT * FROM PatientRecyclingOrder WHERE seq >= "
        "(SELECT seq FROM PatientRecyclingOrder WHERE patientid=${id}) ORDER BY seq LIMIT 2");

      statement.SetReadOnly(true);
      statement.SetParameterType("id", ValueType_Integer64);

      Dictionary args;
      args.SetIntegerValue("id", patient);

      statement.Execute(args);
      
      if (statement.IsDone())
      {
        // The patient is protected, don't add it to the recycling order
        return;
      }

      seq = ReadInteger64(statement, 0);

      statement.Next();

      if (statement.IsDone())
      {
        // The patient is already at the end of the recycling order
        // (because of the "LIMIT 2" above), no need to modify the table
        return;
      }
    }

    // Delete the old position of the patient in the recycling order

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "DELETE FROM PatientRecyclingOrder WHERE seq=${seq}");
        
      statement.SetParameterType("seq", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("seq", seq);
        
      statement.Execute(args);
    }

    // Add the patient to the end of the recycling order

    {
      DatabaseManager::CachedStatement statement(
        STATEMENT_FROM_HERE, manager,
        "INSERT INTO PatientRecyclingOrder VALUES(${}, ${id})");
        
      statement.SetParameterType("id", ValueType_Integer64);
        
      Dictionary args;
      args.SetIntegerValue("id", patient);
        
      statement.Execute(args);
    }
  }


#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
  // New primitive since Orthanc 1.5.4
bool IndexBackend::LookupResourceAndParent(int64_t& id,
                                           OrthancPluginResourceType& type,
                                           std::string& parentPublicId,
                                           DatabaseManager& manager,
                                           const char* publicId)
{
  DatabaseManager::CachedStatement statement(
    STATEMENT_FROM_HERE, manager,
    "SELECT resource.internalId, resource.resourceType, parent.publicId "
    "FROM Resources AS resource LEFT JOIN Resources parent ON parent.internalId=resource.parentId "
    "WHERE resource.publicId=${id}");

  statement.SetParameterType("id", ValueType_Utf8String);
        
  Dictionary args;
  args.SetUtf8Value("id", publicId);

  statement.Execute(args);

  if (statement.IsDone())
  {
    return false;
  }
  else
  {
    if (statement.GetResultFieldsCount() != 3)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }

    statement.SetResultFieldType(0, ValueType_Integer64);
    statement.SetResultFieldType(1, ValueType_Integer64);      
    statement.SetResultFieldType(2, ValueType_Utf8String);

    id = ReadInteger64(statement, 0);
    type = static_cast<OrthancPluginResourceType>(ReadInteger32(statement, 1));

    const IValue& value = statement.GetResultField(2);
      
    switch (value.GetType())
    {
      case ValueType_Null:
        parentPublicId.clear();
        break;

      case ValueType_Utf8String:
        parentPublicId = dynamic_cast<const Utf8StringValue&>(value).GetContent();
        break;

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
      
    assert((statement.Next(), statement.IsDone()));
    return true;
  }
}
#  endif
#endif
  

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)      // Macro introduced in 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 5, 4)
  // New primitive since Orthanc 1.5.4
  void IndexBackend::GetAllMetadata(std::map<int32_t, std::string>& result,
                                    DatabaseManager& manager,
                                    int64_t id)
  {
    DatabaseManager::CachedStatement statement(
      STATEMENT_FROM_HERE, manager,
      "SELECT type, value FROM Metadata WHERE id=${id}");
      
    statement.SetReadOnly(true);
    statement.SetParameterType("id", ValueType_Integer64);

    Dictionary args;
    args.SetIntegerValue("id", id);

    statement.Execute(args);
      
    result.clear();

    if (!statement.IsDone())
    {
      if (statement.GetResultFieldsCount() != 2)
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }
      
      statement.SetResultFieldType(0, ValueType_Integer64);
      statement.SetResultFieldType(1, ValueType_Utf8String);

      while (!statement.IsDone())
      {
        result[ReadInteger32(statement, 0)] = ReadString(statement, 1);
        statement.Next();
      }
    }
  }
#  endif
#endif


  void IndexBackend::CreateInstanceGeneric(OrthancPluginCreateInstanceResult& result,
                                           DatabaseManager& manager,
                                           const char* hashPatient,
                                           const char* hashStudy,
                                           const char* hashSeries,
                                           const char* hashInstance)
  {
    // Check out "OrthancServer/Sources/Database/Compatibility/ICreateInstance.cpp"
    
    {
      OrthancPluginResourceType type;
      int64_t tmp;
        
      if (LookupResource(tmp, type, manager, hashInstance))
      {
        // The instance already exists
        assert(type == OrthancPluginResourceType_Instance);
        result.instanceId = tmp;
        result.isNewInstance = false;
        return;
      }
    }

    result.instanceId = CreateResource(manager, hashInstance, OrthancPluginResourceType_Instance);
    result.isNewInstance = true;

    result.isNewPatient = false;
    result.isNewStudy = false;
    result.isNewSeries = false;
    result.patientId = -1;
    result.studyId = -1;
    result.seriesId = -1;
      
    // Detect up to which level the patient/study/series/instance
    // hierarchy must be created

    {
      OrthancPluginResourceType dummy;

      if (LookupResource(result.seriesId, dummy, manager, hashSeries))
      {
        assert(dummy == OrthancPluginResourceType_Series);
        // The patient, the study and the series already exist

        bool ok = (LookupResource(result.patientId, dummy, manager, hashPatient) &&
                   LookupResource(result.studyId, dummy, manager, hashStudy));
        (void) ok;  // Remove warning about unused variable in release builds
        assert(ok);
      }
      else if (LookupResource(result.studyId, dummy, manager, hashStudy))
      {
        assert(dummy == OrthancPluginResourceType_Study);

        // New series: The patient and the study already exist
        result.isNewSeries = true;

        bool ok = LookupResource(result.patientId, dummy, manager, hashPatient);
        (void) ok;  // Remove warning about unused variable in release builds
        assert(ok);
      }
      else if (LookupResource(result.patientId, dummy, manager, hashPatient))
      {
        assert(dummy == OrthancPluginResourceType_Patient);

        // New study and series: The patient already exist
        result.isNewStudy = true;
        result.isNewSeries = true;
      }
      else
      {
        // New patient, study and series: Nothing exists
        result.isNewPatient = true;
        result.isNewStudy = true;
        result.isNewSeries = true;
      }
    }

    // Create the series if needed
    if (result.isNewSeries)
    {
      result.seriesId = CreateResource(manager, hashSeries, OrthancPluginResourceType_Series);
    }

    // Create the study if needed
    if (result.isNewStudy)
    {
      result.studyId = CreateResource(manager, hashStudy, OrthancPluginResourceType_Study);
    }

    // Create the patient if needed
    if (result.isNewPatient)
    {
      result.patientId = CreateResource(manager, hashPatient, OrthancPluginResourceType_Patient);
    }

    // Create the parent-to-child links
    AttachChild(manager, result.seriesId, result.instanceId);

    if (result.isNewSeries)
    {
      AttachChild(manager, result.studyId, result.seriesId);
    }

    if (result.isNewStudy)
    {
      AttachChild(manager, result.patientId, result.studyId);
    }

    TagMostRecentPatient(manager, result.patientId);
      
    // Sanity checks
    assert(result.patientId != -1);
    assert(result.studyId != -1);
    assert(result.seriesId != -1);
    assert(result.instanceId != -1);
  }


  void IndexBackend::Register(IndexBackend* backend,
                              size_t countConnections,
                              unsigned int maxDatabaseRetries)
  {
    if (backend == NULL)
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_NullPointer);
    }
    
    bool hasLoadedV3 = false;
      
#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 2)
    if (OrthancPluginCheckVersionAdvanced(backend->GetContext(), 1, 9, 2) == 1)
    {
      LOG(WARNING) << "The index plugin will use " << countConnections << " connection(s) to the database, "
                   << "and will retry up to " << maxDatabaseRetries << " time(s) in the case of a collision";
      
      OrthancDatabases::DatabaseBackendAdapterV3::Register(backend, countConnections, maxDatabaseRetries);
      hasLoadedV3 = true;
    }
#  endif
#endif

    if (!hasLoadedV3)
    {
      LOG(WARNING) << "Performance warning: Your version of the Orthanc SDK doesn't support multiple readers/writers";
      OrthancDatabases::DatabaseBackendAdapterV2::Register(backend);
    }
  }


  bool IndexBackend::LookupGlobalIntegerProperty(int& target,
                                                 DatabaseManager& manager,
                                                 const char* serverIdentifier,
                                                 int32_t property)
  {
    std::string value;

    if (LookupGlobalProperty(value, manager, serverIdentifier, property))
    {
      try
      {
        target = boost::lexical_cast<int>(value);
        return true;
      }
      catch (boost::bad_lexical_cast&)
      {
        LOG(ERROR) << "Corrupted PostgreSQL database";
        throw Orthanc::OrthancException(Orthanc::ErrorCode_Database);
      }      
    }
    else
    {
      return false;
    }    
  }
  

  void IndexBackend::SetGlobalIntegerProperty(DatabaseManager& manager,
                                              const char* serverIdentifier,
                                              int32_t property,
                                              int value)
  {
    std::string s = boost::lexical_cast<std::string>(value);
    SetGlobalProperty(manager, serverIdentifier, property, s.c_str());
  }
  

  void IndexBackend::Finalize()
  {
    OrthancDatabases::DatabaseBackendAdapterV2::Finalize();

#if defined(ORTHANC_PLUGINS_VERSION_IS_ABOVE)         // Macro introduced in Orthanc 1.3.1
#  if ORTHANC_PLUGINS_VERSION_IS_ABOVE(1, 9, 2)
    OrthancDatabases::DatabaseBackendAdapterV3::Finalize();
#  endif
#endif
  }


  DatabaseManager* IndexBackend::CreateSingleDatabaseManager(IDatabaseBackend& backend)
  {
    std::unique_ptr<DatabaseManager> manager(new DatabaseManager(backend.CreateDatabaseFactory()));
    backend.ConfigureDatabase(*manager);
    return manager.release();
  }
}
