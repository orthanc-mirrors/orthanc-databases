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

#include "../Common/ImplicitTransaction.h"
#include "DatabaseBackendAdapterV2.h"
#include "GlobalProperties.h"

#include <Compatibility.h>  // For std::unique_ptr<>

#include <orthanc/OrthancCDatabasePlugin.h>

#include <gtest/gtest.h>
#include <list>


namespace Orthanc
{
  /**
   * Mock enumeration inspired from the source code of Orthanc... only
   * for use in the unit tests!
   * https://hg.orthanc-server.com/orthanc/file/default/OrthancServer/Sources/ServerEnumerations.h
   **/
  enum MetadataType
  {
    MetadataType_ModifiedFrom,
    MetadataType_LastUpdate
  };
}


static std::unique_ptr<OrthancPluginAttachment>  expectedAttachment;
static std::list<OrthancPluginDicomTag>  expectedDicomTags;
static std::unique_ptr<OrthancPluginExportedResource>  expectedExported;

static void CheckAttachment(const OrthancPluginAttachment& attachment)
{
  ASSERT_STREQ(expectedAttachment->uuid, attachment.uuid);
  ASSERT_EQ(expectedAttachment->contentType, attachment.contentType);
  ASSERT_EQ(expectedAttachment->uncompressedSize, attachment.uncompressedSize);
  ASSERT_STREQ(expectedAttachment->uncompressedHash, attachment.uncompressedHash);
  ASSERT_EQ(expectedAttachment->compressionType, attachment.compressionType);
  ASSERT_EQ(expectedAttachment->compressedSize, attachment.compressedSize);
  ASSERT_STREQ(expectedAttachment->compressedHash, attachment.compressedHash);
}

static void CheckExportedResource(const OrthancPluginExportedResource& exported)
{
  ASSERT_EQ(expectedExported->seq, exported.seq);
  ASSERT_EQ(expectedExported->resourceType, exported.resourceType);
  ASSERT_STREQ(expectedExported->publicId, exported.publicId);
  ASSERT_STREQ(expectedExported->modality, exported.modality);
  ASSERT_STREQ(expectedExported->date, exported.date);
  ASSERT_STREQ(expectedExported->patientId, exported.patientId);
  ASSERT_STREQ(expectedExported->studyInstanceUid, exported.studyInstanceUid);
  ASSERT_STREQ(expectedExported->seriesInstanceUid, exported.seriesInstanceUid);
  ASSERT_STREQ(expectedExported->sopInstanceUid, exported.sopInstanceUid);
}

static void CheckDicomTag(const OrthancPluginDicomTag& tag)
{
  for (std::list<OrthancPluginDicomTag>::const_iterator
         it = expectedDicomTags.begin(); it != expectedDicomTags.end(); ++it)
  {
    if (it->group == tag.group &&
        it->element == tag.element &&
        !strcmp(it->value, tag.value))
    {
      // OK, match
      return;
    }
  }

  ASSERT_TRUE(0);  // Error
}



static OrthancPluginErrorCode InvokeService(struct _OrthancPluginContext_t* context,
                                            _OrthancPluginService service,
                                            const void* params)
{
  switch (service)
  {
    case _OrthancPluginService_DatabaseAnswer:
    {
      const _OrthancPluginDatabaseAnswer& answer = 
        *reinterpret_cast<const _OrthancPluginDatabaseAnswer*>(params);

      switch (answer.type)
      {
        case _OrthancPluginDatabaseAnswerType_Attachment:
        {
          const OrthancPluginAttachment& attachment = 
            *reinterpret_cast<const OrthancPluginAttachment*>(answer.valueGeneric);
          CheckAttachment(attachment);
          break;
        }

        case _OrthancPluginDatabaseAnswerType_ExportedResource:
        {
          const OrthancPluginExportedResource& attachment = 
            *reinterpret_cast<const OrthancPluginExportedResource*>(answer.valueGeneric);
          CheckExportedResource(attachment);
          break;
        }

        case _OrthancPluginDatabaseAnswerType_DicomTag:
        {
          const OrthancPluginDicomTag& tag = 
            *reinterpret_cast<const OrthancPluginDicomTag*>(answer.valueGeneric);
          CheckDicomTag(tag);
          break;
        }

        default:
          printf("Unhandled message: %d\n", answer.type);
          break;
      }

      return OrthancPluginErrorCode_Success;
    }

    case _OrthancPluginService_GetExpectedDatabaseVersion:
    {
      const _OrthancPluginReturnSingleValue& p =
        *reinterpret_cast<const _OrthancPluginReturnSingleValue*>(params);
      *(p.resultUint32) = ORTHANC_DATABASE_VERSION;
      return OrthancPluginErrorCode_Success;
    }

    default:
      assert(0);
      printf("Service not emulated: %d\n", service);
      return OrthancPluginErrorCode_NotImplemented;
  }
}


TEST(IndexBackend, Basic)
{
  using namespace OrthancDatabases;

  OrthancPluginContext context;
  context.pluginsManager = NULL;
  context.orthancVersion = "mainline";
  context.Free = ::free;
  context.InvokeService = InvokeService;

  ImplicitTransaction::SetErrorOnDoubleExecution(true);

#if ORTHANC_ENABLE_POSTGRESQL == 1
  PostgreSQLIndex db(&context, globalParameters_);
  db.SetClearAll(true);
#elif ORTHANC_ENABLE_MYSQL == 1
  MySQLIndex db(&context, globalParameters_);
  db.SetClearAll(true);
#elif ORTHANC_ENABLE_SQLITE == 1
  SQLiteIndex db(&context);  // Open in memory
#else
#  error Unsupported database backend
#endif

  db.SetOutputFactory(new DatabaseBackendAdapterV2::Factory(&context, NULL));

  std::unique_ptr<DatabaseManager> manager(IndexBackend::CreateSingleDatabaseManager(db));
  
  std::unique_ptr<IDatabaseBackendOutput> output(db.CreateOutput());

  std::string s;
  ASSERT_TRUE(db.LookupGlobalProperty(s, *manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseSchemaVersion));
  ASSERT_EQ("6", s);

  ASSERT_FALSE(db.LookupGlobalProperty(s, *manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseInternal9));
  db.SetGlobalProperty(*manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseInternal9, "Hello");
  ASSERT_TRUE(db.LookupGlobalProperty(s, *manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseInternal9));
  ASSERT_EQ("Hello", s);
  db.SetGlobalProperty(*manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseInternal9, "HelloWorld");
  ASSERT_TRUE(db.LookupGlobalProperty(s, *manager, MISSING_SERVER_IDENTIFIER, Orthanc::GlobalProperty_DatabaseInternal9));
  ASSERT_EQ("HelloWorld", s);

  int64_t a = db.CreateResource(*manager, "study", OrthancPluginResourceType_Study);
  ASSERT_TRUE(db.IsExistingResource(*manager, a));
  ASSERT_FALSE(db.IsExistingResource(*manager, a + 1));

  int64_t b;
  OrthancPluginResourceType t;
  ASSERT_FALSE(db.LookupResource(b, t, *manager, "world"));
  ASSERT_TRUE(db.LookupResource(b, t, *manager, "study"));
  ASSERT_EQ(a, b);
  ASSERT_EQ(OrthancPluginResourceType_Study, t);
  
  b = db.CreateResource(*manager, "series", OrthancPluginResourceType_Series);
  ASSERT_NE(a, b);

  ASSERT_EQ("study", db.GetPublicId(*manager, a));
  ASSERT_EQ("series", db.GetPublicId(*manager, b));
  ASSERT_EQ(OrthancPluginResourceType_Study, db.GetResourceType(*manager, a));
  ASSERT_EQ(OrthancPluginResourceType_Series, db.GetResourceType(*manager, b));

  db.AttachChild(*manager, a, b);

  int64_t c;
  ASSERT_FALSE(db.LookupParent(c, *manager, a));
  ASSERT_TRUE(db.LookupParent(c, *manager, b));
  ASSERT_EQ(a, c);

  c = db.CreateResource(*manager, "series2", OrthancPluginResourceType_Series);
  db.AttachChild(*manager, a, c);

  ASSERT_EQ(3u, db.GetAllResourcesCount(*manager));
  ASSERT_EQ(0u, db.GetResourcesCount(*manager, OrthancPluginResourceType_Patient));
  ASSERT_EQ(1u, db.GetResourcesCount(*manager, OrthancPluginResourceType_Study));
  ASSERT_EQ(2u, db.GetResourcesCount(*manager, OrthancPluginResourceType_Series));

  ASSERT_FALSE(db.GetParentPublicId(s, *manager, a));
  ASSERT_TRUE(db.GetParentPublicId(s, *manager, b));  ASSERT_EQ("study", s);
  ASSERT_TRUE(db.GetParentPublicId(s, *manager, c));  ASSERT_EQ("study", s);

  std::list<std::string> children;
  db.GetChildren(children, *manager, a);
  ASSERT_EQ(2u, children.size());
  db.GetChildren(children, *manager, b);
  ASSERT_EQ(0u, children.size());
  db.GetChildren(children, *manager, c);
  ASSERT_EQ(0u, children.size());

  std::list<std::string> cp;
  db.GetChildrenPublicId(cp, *manager, a);
  ASSERT_EQ(2u, cp.size());
  ASSERT_TRUE(cp.front() == "series" || cp.front() == "series2");
  ASSERT_TRUE(cp.back() == "series" || cp.back() == "series2");
  ASSERT_NE(cp.front(), cp.back());

  std::list<std::string> pub;
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Patient);
  ASSERT_EQ(0u, pub.size());
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Study);
  ASSERT_EQ(1u, pub.size());
  ASSERT_EQ("study", pub.front());
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Series);
  ASSERT_EQ(2u, pub.size());
  ASSERT_TRUE(pub.front() == "series" || pub.front() == "series2");
  ASSERT_TRUE(pub.back() == "series" || pub.back() == "series2");
  ASSERT_NE(pub.front(), pub.back());

  std::list<int64_t> ci;
  db.GetChildrenInternalId(ci, *manager, a);
  ASSERT_EQ(2u, ci.size());
  ASSERT_TRUE(ci.front() == b || ci.front() == c);
  ASSERT_TRUE(ci.back() == b || ci.back() == c);
  ASSERT_NE(ci.front(), ci.back());

  db.SetMetadata(*manager, a, Orthanc::MetadataType_ModifiedFrom, "modified", 42);
  db.SetMetadata(*manager, a, Orthanc::MetadataType_LastUpdate, "update2", 43);
  int64_t revision = -1;
  ASSERT_FALSE(db.LookupMetadata(s, revision, *manager, b, Orthanc::MetadataType_LastUpdate));
  ASSERT_TRUE(db.LookupMetadata(s, revision, *manager, a, Orthanc::MetadataType_LastUpdate));
  ASSERT_EQ("update2", s);

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(43, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  db.SetMetadata(*manager, a, Orthanc::MetadataType_LastUpdate, "update", 44);
  ASSERT_TRUE(db.LookupMetadata(s, revision, *manager, a, Orthanc::MetadataType_LastUpdate));
  ASSERT_EQ("update", s);

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(44, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  std::list<int32_t> md;
  db.ListAvailableMetadata(md, *manager, a);
  ASSERT_EQ(2u, md.size());
  ASSERT_TRUE(md.front() == Orthanc::MetadataType_ModifiedFrom || md.back() == Orthanc::MetadataType_ModifiedFrom);
  ASSERT_TRUE(md.front() == Orthanc::MetadataType_LastUpdate || md.back() == Orthanc::MetadataType_LastUpdate);
  std::string mdd;
  ASSERT_TRUE(db.LookupMetadata(mdd, revision, *manager, a, Orthanc::MetadataType_ModifiedFrom));
  ASSERT_EQ("modified", mdd);

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(42, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  ASSERT_TRUE(db.LookupMetadata(mdd, revision, *manager, a, Orthanc::MetadataType_LastUpdate));
  ASSERT_EQ("update", mdd);

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(44, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  db.ListAvailableMetadata(md, *manager, b);
  ASSERT_EQ(0u, md.size());

  ASSERT_TRUE(db.LookupMetadata(s, revision, *manager, a, Orthanc::MetadataType_LastUpdate));
  db.DeleteMetadata(*manager, a, Orthanc::MetadataType_LastUpdate);
  ASSERT_FALSE(db.LookupMetadata(s, revision, *manager, a, Orthanc::MetadataType_LastUpdate));
  db.DeleteMetadata(*manager, b, Orthanc::MetadataType_LastUpdate);
  ASSERT_FALSE(db.LookupMetadata(s, revision, *manager, a, Orthanc::MetadataType_LastUpdate));

  db.ListAvailableMetadata(md, *manager, a);
  ASSERT_EQ(1u, md.size());
  ASSERT_EQ(Orthanc::MetadataType_ModifiedFrom, md.front());

  ASSERT_EQ(0u, db.GetTotalCompressedSize(*manager));
  ASSERT_EQ(0u, db.GetTotalUncompressedSize(*manager));


  std::list<int32_t> fc;

  OrthancPluginAttachment a1;
  a1.uuid = "uuid1";
  a1.contentType = Orthanc::FileContentType_Dicom;
  a1.uncompressedSize = 42;
  a1.uncompressedHash = "md5_1";
  a1.compressionType = Orthanc::CompressionType_None;
  a1.compressedSize = 42;
  a1.compressedHash = "md5_1";
    
  OrthancPluginAttachment a2;
  a2.uuid = "uuid2";
  a2.contentType = Orthanc::FileContentType_DicomAsJson;
  a2.uncompressedSize = 4242;
  a2.uncompressedHash = "md5_2";
  a2.compressionType = Orthanc::CompressionType_None;
  a2.compressedSize = 4242;
  a2.compressedHash = "md5_2";
    
  db.AddAttachment(*manager, a, a1, 42);
  db.ListAvailableAttachments(fc, *manager, a);
  ASSERT_EQ(1u, fc.size());
  ASSERT_EQ(Orthanc::FileContentType_Dicom, fc.front());
  db.AddAttachment(*manager, a, a2, 43);
  db.ListAvailableAttachments(fc, *manager, a);
  ASSERT_EQ(2u, fc.size());
  ASSERT_FALSE(db.LookupAttachment(*output, revision, *manager, b, Orthanc::FileContentType_Dicom));

  ASSERT_EQ(4284u, db.GetTotalCompressedSize(*manager));
  ASSERT_EQ(4284u, db.GetTotalUncompressedSize(*manager));

  expectedAttachment.reset(new OrthancPluginAttachment);
  expectedAttachment->uuid = "uuid1";
  expectedAttachment->contentType = Orthanc::FileContentType_Dicom;
  expectedAttachment->uncompressedSize = 42;
  expectedAttachment->uncompressedHash = "md5_1";
  expectedAttachment->compressionType = Orthanc::CompressionType_None;
  expectedAttachment->compressedSize = 42;
  expectedAttachment->compressedHash = "md5_1";
  ASSERT_TRUE(db.LookupAttachment(*output, revision, *manager, a, Orthanc::FileContentType_Dicom));

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(42, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  expectedAttachment.reset(new OrthancPluginAttachment);
  expectedAttachment->uuid = "uuid2";
  expectedAttachment->contentType = Orthanc::FileContentType_DicomAsJson;
  expectedAttachment->uncompressedSize = 4242;
  expectedAttachment->uncompressedHash = "md5_2";
  expectedAttachment->compressionType = Orthanc::CompressionType_None;
  expectedAttachment->compressedSize = 4242;
  expectedAttachment->compressedHash = "md5_2";
  revision = -1;
  ASSERT_TRUE(db.LookupAttachment(*output, revision, *manager, a, Orthanc::FileContentType_DicomAsJson));

#if ORTHANC_ENABLE_SQLITE == 1
  ASSERT_EQ(43, revision);  // Only SQLite implements revisions so far
#else
  ASSERT_EQ(0, revision);
#endif

  db.ListAvailableAttachments(fc, *manager, b);
  ASSERT_EQ(0u, fc.size());
  db.DeleteAttachment(*output, *manager, a, Orthanc::FileContentType_Dicom);
  db.ListAvailableAttachments(fc, *manager, a);
  ASSERT_EQ(1u, fc.size());
  ASSERT_EQ(Orthanc::FileContentType_DicomAsJson, fc.front());
  db.DeleteAttachment(*output, *manager, a, Orthanc::FileContentType_DicomAsJson);
  db.ListAvailableAttachments(fc, *manager, a);
  ASSERT_EQ(0u, fc.size());


  db.SetIdentifierTag(*manager, a, 0x0010, 0x0020, "patient");
  db.SetIdentifierTag(*manager, a, 0x0020, 0x000d, "study");

  expectedDicomTags.clear();
  expectedDicomTags.push_back(OrthancPluginDicomTag());
  expectedDicomTags.push_back(OrthancPluginDicomTag());
  expectedDicomTags.front().group = 0x0010;
  expectedDicomTags.front().element = 0x0020;
  expectedDicomTags.front().value = "patient";
  expectedDicomTags.back().group = 0x0020;
  expectedDicomTags.back().element = 0x000d;
  expectedDicomTags.back().value = "study";
  db.GetMainDicomTags(*output, *manager, a);


  db.LookupIdentifier(ci, *manager, OrthancPluginResourceType_Study, 0x0010, 0x0020, 
                      OrthancPluginIdentifierConstraint_Equal, "patient");
  ASSERT_EQ(1u, ci.size());
  ASSERT_EQ(a, ci.front());
  db.LookupIdentifier(ci, *manager, OrthancPluginResourceType_Study, 0x0010, 0x0020, 
                      OrthancPluginIdentifierConstraint_Equal, "study");
  ASSERT_EQ(0u, ci.size());


  OrthancPluginExportedResource exp;
  exp.seq = -1;
  exp.resourceType = OrthancPluginResourceType_Study;
  exp.publicId = "id";
  exp.modality = "remote";
  exp.date = "date";
  exp.patientId = "patient";
  exp.studyInstanceUid = "study";
  exp.seriesInstanceUid = "series";
  exp.sopInstanceUid = "instance";
  db.LogExportedResource(*manager, exp);

  expectedExported.reset(new OrthancPluginExportedResource());
  *expectedExported = exp;
  expectedExported->seq = 1;

  bool done;
  db.GetExportedResources(*output, done, *manager, 0, 10);
  

  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Patient); ASSERT_EQ(0u, pub.size());
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Study); ASSERT_EQ(1u, pub.size());
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Series); ASSERT_EQ(2u, pub.size());
  db.GetAllPublicIds(pub, *manager, OrthancPluginResourceType_Instance); ASSERT_EQ(0u, pub.size());
  ASSERT_EQ(3u, db.GetAllResourcesCount(*manager));

  ASSERT_EQ(0u, db.GetUnprotectedPatientsCount(*manager));  // No patient was inserted
  ASSERT_TRUE(db.IsExistingResource(*manager, c));

  {
    // A transaction is needed here for MySQL, as it was not possible
    // to implement recursive deletion of resources using pure SQL
    // statements
    manager->StartTransaction(TransactionType_ReadWrite);    
    db.DeleteResource(*output, *manager, c);
    manager->CommitTransaction();
  }
  
  ASSERT_FALSE(db.IsExistingResource(*manager, c));
  ASSERT_TRUE(db.IsExistingResource(*manager, a));
  ASSERT_TRUE(db.IsExistingResource(*manager, b));
  ASSERT_EQ(2u, db.GetAllResourcesCount(*manager));
  db.DeleteResource(*output, *manager, a);
  ASSERT_EQ(0u, db.GetAllResourcesCount(*manager));
  ASSERT_FALSE(db.IsExistingResource(*manager, a));
  ASSERT_FALSE(db.IsExistingResource(*manager, b));
  ASSERT_FALSE(db.IsExistingResource(*manager, c));

  ASSERT_EQ(0u, db.GetAllResourcesCount(*manager));
  ASSERT_EQ(0u, db.GetUnprotectedPatientsCount(*manager));
  int64_t p1 = db.CreateResource(*manager, "patient1", OrthancPluginResourceType_Patient);
  int64_t p2 = db.CreateResource(*manager, "patient2", OrthancPluginResourceType_Patient);
  int64_t p3 = db.CreateResource(*manager, "patient3", OrthancPluginResourceType_Patient);
  ASSERT_EQ(3u, db.GetUnprotectedPatientsCount(*manager));
  int64_t r;
  ASSERT_TRUE(db.SelectPatientToRecycle(r, *manager));
  ASSERT_EQ(p1, r);
  ASSERT_TRUE(db.SelectPatientToRecycle(r, *manager, p1));
  ASSERT_EQ(p2, r);
  ASSERT_FALSE(db.IsProtectedPatient(*manager, p1));
  db.SetProtectedPatient(*manager, p1, true);
  ASSERT_TRUE(db.IsProtectedPatient(*manager, p1));
  ASSERT_TRUE(db.SelectPatientToRecycle(r, *manager));
  ASSERT_EQ(p2, r);
  db.SetProtectedPatient(*manager, p1, false);
  ASSERT_FALSE(db.IsProtectedPatient(*manager, p1));
  ASSERT_TRUE(db.SelectPatientToRecycle(r, *manager));
  ASSERT_EQ(p2, r);
  db.DeleteResource(*output, *manager, p2);
  ASSERT_TRUE(db.SelectPatientToRecycle(r, *manager, p3));
  ASSERT_EQ(p1, r);

  manager->Close();
}
