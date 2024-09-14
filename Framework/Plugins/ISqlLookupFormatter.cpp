/**
 * Orthanc - A Lightweight, RESTful DICOM Store
 * Copyright (C) 2012-2016 Sebastien Jodogne, Medical Physics
 * Department, University Hospital of Liege, Belgium
 * Copyright (C) 2017-2023 Osimis S.A., Belgium
 * Copyright (C) 2024-2024 Orthanc Team SRL, Belgium
 * Copyright (C) 2021-2024 Sebastien Jodogne, ICTEAM UCLouvain, Belgium
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


/**
 * NB: Until 2024-09-09, this file was synchronized with the following
 * folder from the Orthanc main project:
 * https://orthanc.uclouvain.be/hg/orthanc/file/default/OrthancServer/Sources/Search/
 **/


#include "ISqlLookupFormatter.h"

#include "DatabaseConstraint.h"

#include <OrthancException.h>
#include <Toolbox.h>

#include <cassert>
#include <boost/lexical_cast.hpp>
#include <list>


namespace OrthancDatabases
{
  static std::string FormatLevel(Orthanc::ResourceType level)
  {
    switch (level)
    {
      case Orthanc::ResourceType_Patient:
        return "patients";

      case Orthanc::ResourceType_Study:
        return "studies";

      case Orthanc::ResourceType_Series:
        return "series";

      case Orthanc::ResourceType_Instance:
        return "instances";

      default:
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }


  static bool FormatComparison(std::string& target,
                               ISqlLookupFormatter& formatter,
                               const DatabaseConstraint& constraint,
                               size_t index,
                               bool escapeBrackets)
  {
    std::string tag = "t" + boost::lexical_cast<std::string>(index);

    std::string comparison;

    switch (constraint.GetConstraintType())
    {
      case ConstraintType_Equal:
      case ConstraintType_SmallerOrEqual:
      case ConstraintType_GreaterOrEqual:
      {
        std::string op;
        switch (constraint.GetConstraintType())
        {
          case ConstraintType_Equal:
            op = "=";
            break;

          case ConstraintType_SmallerOrEqual:
            op = "<=";
            break;

          case ConstraintType_GreaterOrEqual:
            op = ">=";
            break;

          default:
            throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
        }

        std::string parameter = formatter.GenerateParameter(constraint.GetSingleValue());

        if (constraint.IsCaseSensitive())
        {
          comparison = tag + ".value " + op + " " + parameter;
        }
        else
        {
          comparison = "lower(" + tag + ".value) " + op + " lower(" + parameter + ")";
        }

        break;
      }

      case ConstraintType_List:
      {
        for (size_t i = 0; i < constraint.GetValuesCount(); i++)
        {
          if (!comparison.empty())
          {
            comparison += ", ";
          }

          std::string parameter = formatter.GenerateParameter(constraint.GetValue(i));

          if (constraint.IsCaseSensitive())
          {
            comparison += parameter;
          }
          else
          {
            comparison += "lower(" + parameter + ")";
          }
        }

        if (constraint.IsCaseSensitive())
        {
          comparison = tag + ".value IN (" + comparison + ")";
        }
        else
        {
          comparison = "lower(" +  tag + ".value) IN (" + comparison + ")";
        }

        break;
      }

      case ConstraintType_Wildcard:
      {
        const std::string value = constraint.GetSingleValue();

        if (value == "*")
        {
          if (!constraint.IsMandatory())
          {
            // Universal constraint on an optional tag, ignore it
            return false;
          }
        }
        else
        {
          std::string escaped;
          escaped.reserve(value.size());

          for (size_t i = 0; i < value.size(); i++)
          {
            if (value[i] == '*')
            {
              escaped += "%";
            }
            else if (value[i] == '?')
            {
              escaped += "_";
            }
            else if (value[i] == '%')
            {
              escaped += "\\%";
            }
            else if (value[i] == '_')
            {
              escaped += "\\_";
            }
            else if (value[i] == '\\')
            {
              escaped += "\\\\";
            }
            else if (escapeBrackets && value[i] == '[')
            {
              escaped += "\\[";
            }
            else if (escapeBrackets && value[i] == ']')
            {
              escaped += "\\]";
            }
            else
            {
              escaped += value[i];
            }
          }

          std::string parameter = formatter.GenerateParameter(escaped);

          if (constraint.IsCaseSensitive())
          {
            comparison = (tag + ".value LIKE " + parameter + " " +
                          formatter.FormatWildcardEscape());
          }
          else
          {
            comparison = ("lower(" + tag + ".value) LIKE lower(" +
                          parameter + ") " + formatter.FormatWildcardEscape());
          }
        }

        break;
      }

      default:
        return false;
    }

    if (constraint.IsMandatory())
    {
      target = comparison;
    }
    else if (comparison.empty())
    {
      target = tag + ".value IS NULL";
    }
    else
    {
      target = tag + ".value IS NULL OR " + comparison;
    }

    return true;
  }


  static void FormatJoin(std::string& target,
                         const DatabaseConstraint& constraint,
                         size_t index)
  {
    std::string tag = "t" + boost::lexical_cast<std::string>(index);

    if (constraint.IsMandatory())
    {
      target = " INNER JOIN ";
    }
    else
    {
      target = " LEFT JOIN ";
    }

    if (constraint.IsIdentifier())
    {
      target += "DicomIdentifiers ";
    }
    else
    {
      target += "MainDicomTags ";
    }

    target += (tag + " ON " + tag + ".id = " + FormatLevel(constraint.GetLevel()) +
               ".internalId AND " + tag + ".tagGroup = " +
               boost::lexical_cast<std::string>(constraint.GetTag().GetGroup()) +
               " AND " + tag + ".tagElement = " +
               boost::lexical_cast<std::string>(constraint.GetTag().GetElement()));
  }


  static std::string Join(const std::list<std::string>& values,
                          const std::string& prefix,
                          const std::string& separator)
  {
    if (values.empty())
    {
      return "";
    }
    else
    {
      std::string s = prefix;

      bool first = true;
      for (std::list<std::string>::const_iterator it = values.begin(); it != values.end(); ++it)
      {
        if (first)
        {
          first = false;
        }
        else
        {
          s += separator;
        }

        s += *it;
      }

      return s;
    }
  }

  static bool FormatComparison2(std::string& target,
                                ISqlLookupFormatter& formatter,
                                const DatabaseConstraint& constraint,
                                bool escapeBrackets)
  {
    std::string comparison;
    std::string tagFilter = ("tagGroup = " + boost::lexical_cast<std::string>(constraint.GetTag().GetGroup())
                              + " AND tagElement = " + boost::lexical_cast<std::string>(constraint.GetTag().GetElement()));

    switch (constraint.GetConstraintType())
    {
      case ConstraintType_Equal:
      case ConstraintType_SmallerOrEqual:
      case ConstraintType_GreaterOrEqual:
      {
        std::string op;
        switch (constraint.GetConstraintType())
        {
          case ConstraintType_Equal:
            op = "=";
            break;

          case ConstraintType_SmallerOrEqual:
            op = "<=";
            break;

          case ConstraintType_GreaterOrEqual:
            op = ">=";
            break;

          default:
            throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
        }

        std::string parameter = formatter.GenerateParameter(constraint.GetSingleValue());

        if (constraint.IsCaseSensitive())
        {
          comparison = " AND value " + op + " " + parameter;
        }
        else
        {
          comparison = " AND lower(value) " + op + " lower(" + parameter + ")";
        }

        break;
      }

      case ConstraintType_List:
      {
        std::vector<std::string> comparisonValues;
        for (size_t i = 0; i < constraint.GetValuesCount(); i++)
        {
          std::string parameter = formatter.GenerateParameter(constraint.GetValue(i));

          if (constraint.IsCaseSensitive())
          {
            comparisonValues.push_back(parameter);
          }
          else
          {
            comparisonValues.push_back("lower(" + parameter + ")");
          }
        }

        std::string values;
        Orthanc::Toolbox::JoinStrings(values, comparisonValues, ", ");

        if (constraint.IsCaseSensitive())
        {
          comparison = " AND value IN (" + values + ")";
        }
        else
        {
          comparison = " AND lower(value) IN (" + values + ")";
        }

        break;
      }

      case ConstraintType_Wildcard:
      {
        const std::string value = constraint.GetSingleValue();

        if (value == "*")
        {
          if (!constraint.IsMandatory())
          {
            // Universal constraint on an optional tag, ignore it
            return false;
          }
        }
        else
        {
          std::string escaped;
          escaped.reserve(value.size());

          for (size_t i = 0; i < value.size(); i++)
          {
            if (value[i] == '*')
            {
              escaped += "%";
            }
            else if (value[i] == '?')
            {
              escaped += "_";
            }
            else if (value[i] == '%')
            {
              escaped += "\\%";
            }
            else if (value[i] == '_')
            {
              escaped += "\\_";
            }
            else if (value[i] == '\\')
            {
              escaped += "\\\\";
            }
            else if (escapeBrackets && value[i] == '[')
            {
              escaped += "\\[";
            }
            else if (escapeBrackets && value[i] == ']')
            {
              escaped += "\\]";
            }
            else
            {
              escaped += value[i];
            }
          }

          std::string parameter = formatter.GenerateParameter(escaped);

          if (constraint.IsCaseSensitive())
          {
            comparison = " AND value LIKE " + parameter + " " + formatter.FormatWildcardEscape();
          }
          else
          {
            comparison = " AND lower(value) LIKE lower(" + parameter + ") " + formatter.FormatWildcardEscape();
          }
        }

        break;
      }

      default:
        return false;
    }

    if (constraint.IsMandatory())
    {
      target = tagFilter + comparison;
    }
    else if (comparison.empty())
    {
      target = tagFilter + " AND value IS NULL";
    }
    else
    {
      target = tagFilter + " AND value IS NULL OR " + comparison;
    }

    return true;
  }


  void ISqlLookupFormatter::GetLookupLevels(Orthanc::ResourceType& lowerLevel,
                                            Orthanc::ResourceType& upperLevel,
                                            const Orthanc::ResourceType& queryLevel,
                                            const DatabaseConstraints& lookup)
  {
    assert(Orthanc::ResourceType_Patient < Orthanc::ResourceType_Study &&
           Orthanc::ResourceType_Study < Orthanc::ResourceType_Series &&
           Orthanc::ResourceType_Series < Orthanc::ResourceType_Instance);

    lowerLevel = queryLevel;
    upperLevel = queryLevel;

    for (size_t i = 0; i < lookup.GetSize(); i++)
    {
      Orthanc::ResourceType level = lookup.GetConstraint(i).GetLevel();

      if (level < upperLevel)
      {
        upperLevel = level;
      }

      if (level > lowerLevel)
      {
        lowerLevel = level;
      }
    }
  }


  void ISqlLookupFormatter::Apply(std::string& sql,
                                  ISqlLookupFormatter& formatter,
                                  const DatabaseConstraints& lookup,
                                  Orthanc::ResourceType queryLevel,
                                  const std::set<std::string>& labels,
                                  LabelsConstraint labelsConstraint,
                                  size_t limit)
  {
    Orthanc::ResourceType lowerLevel, upperLevel;
    GetLookupLevels(lowerLevel, upperLevel, queryLevel, lookup);

    assert(upperLevel <= queryLevel &&
           queryLevel <= lowerLevel);

    const bool escapeBrackets = formatter.IsEscapeBrackets();

    std::string joins, comparisons;

    size_t count = 0;

    for (size_t i = 0; i < lookup.GetSize(); i++)
    {
      const DatabaseConstraint& constraint = lookup.GetConstraint(i);

      std::string comparison;

      if (FormatComparison(comparison, formatter, constraint, count, escapeBrackets))
      {
        std::string join;
        FormatJoin(join, constraint, count);
        joins += join;

        if (!comparison.empty())
        {
          comparisons += " AND " + comparison;
        }

        count ++;
      }
    }

    sql = ("SELECT " +
           FormatLevel(queryLevel) + ".publicId, " +
           FormatLevel(queryLevel) + ".internalId" +
           " FROM Resources AS " + FormatLevel(queryLevel));

    for (int level = queryLevel - 1; level >= upperLevel; level--)
    {
      sql += (" INNER JOIN Resources " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + " ON " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + ".internalId=" +
              FormatLevel(static_cast<Orthanc::ResourceType>(level + 1)) + ".parentId");
    }

    for (int level = queryLevel + 1; level <= lowerLevel; level++)
    {
      sql += (" INNER JOIN Resources " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + " ON " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level - 1)) + ".internalId=" +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + ".parentId");
    }

    std::list<std::string> where;
    where.push_back(FormatLevel(queryLevel) + ".resourceType = " +
                    formatter.FormatResourceType(queryLevel) + comparisons);

    if (!labels.empty())
    {
      /**
       * "In SQL Server, NOT EXISTS and NOT IN predicates are the best
       * way to search for missing values, as long as both columns in
       * question are NOT NULL."
       * https://explainextended.com/2009/09/15/not-in-vs-not-exists-vs-left-join-is-null-sql-server/
       **/

      std::list<std::string> formattedLabels;
      for (std::set<std::string>::const_iterator it = labels.begin(); it != labels.end(); ++it)
      {
        formattedLabels.push_back(formatter.GenerateParameter(*it));
      }

      std::string condition;
      switch (labelsConstraint)
      {
        case LabelsConstraint_Any:
          condition = "> 0";
          break;

        case LabelsConstraint_All:
          condition = "= " + boost::lexical_cast<std::string>(labels.size());
          break;

        case LabelsConstraint_None:
          condition = "= 0";
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
      }

      where.push_back("(SELECT COUNT(1) FROM Labels AS selectedLabels WHERE selectedLabels.id = " + FormatLevel(queryLevel) +
                      ".internalId AND selectedLabels.label IN (" + Join(formattedLabels, "", ", ") + ")) " + condition);
    }

    sql += joins + Join(where, " WHERE ", " AND ");

    if (limit != 0)
    {
      sql += " LIMIT " + boost::lexical_cast<std::string>(limit);
    }
  }


#if ORTHANC_PLUGINS_HAS_INTEGRATED_FIND == 1
  static Orthanc::ResourceType DetectLevel(const Orthanc::DatabasePluginMessages::Find_Request& request)
  {
    // This corresponds to "Orthanc::OrthancIdentifiers()::DetectLevel()" in the Orthanc core
    if (!request.orthanc_id_patient().empty() &&
        request.orthanc_id_study().empty() &&
        request.orthanc_id_series().empty() &&
        request.orthanc_id_instance().empty())
    {
      return Orthanc::ResourceType_Patient;
    }
    else if (!request.orthanc_id_study().empty() &&
             request.orthanc_id_series().empty() &&
             request.orthanc_id_instance().empty())
    {
      return Orthanc::ResourceType_Study;
    }
    else if (!request.orthanc_id_series().empty() &&
             request.orthanc_id_instance().empty())
    {
      return Orthanc::ResourceType_Series;
    }
    else if (!request.orthanc_id_instance().empty())
    {
      return Orthanc::ResourceType_Instance;
    }
    else
    {
      throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
    }
  }

  void ISqlLookupFormatter::Apply(std::string& sql,
                                  ISqlLookupFormatter& formatter,
                                  const Orthanc::DatabasePluginMessages::Find_Request& request)
  {
    const bool escapeBrackets = formatter.IsEscapeBrackets();
    Orthanc::ResourceType queryLevel = MessagesToolbox::Convert(request.level());
    const std::string& strQueryLevel = FormatLevel(queryLevel);

    DatabaseConstraints constraints;

    for (int i = 0; i < request.dicom_tag_constraints().size(); i++)
    {
      constraints.AddConstraint(new DatabaseConstraint(request.dicom_tag_constraints(i)));
    }

    Orthanc::ResourceType lowerLevel, upperLevel;
    GetLookupLevels(lowerLevel, upperLevel, queryLevel, constraints);

    assert(upperLevel <= queryLevel &&
           queryLevel <= lowerLevel);

    std::string ordering = "row_number() over (order by " + strQueryLevel + ".publicId) as rowNumber";  // we need a default ordering in order to make default queries repeatable when using since&limit

    sql = ("SELECT " +
           strQueryLevel + ".publicId, " +
           strQueryLevel + ".internalId, " +
           ordering +
           " FROM Resources AS " + strQueryLevel);


    std::string joins, comparisons;

    const bool isOrthancIdentifiersDefined = (!request.orthanc_id_patient().empty() ||
                                              !request.orthanc_id_study().empty() ||
                                              !request.orthanc_id_series().empty() ||
                                              !request.orthanc_id_instance().empty());

    if (isOrthancIdentifiersDefined &&
        Orthanc::IsResourceLevelAboveOrEqual(DetectLevel(request), queryLevel))
    {
      // single child resource matching, there should not be other constraints (at least for now)
      if (request.dicom_tag_constraints().size() != 0 ||
          request.labels().size() != 0 ||
          request.has_limits())
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_NotImplemented);
      }

      Orthanc::ResourceType topParentLevel = DetectLevel(request);
      const std::string& strTopParentLevel = FormatLevel(topParentLevel);

      std::string publicId;
      switch (topParentLevel)
      {
        case Orthanc::ResourceType_Patient:
          publicId = request.orthanc_id_patient();
          break;

        case Orthanc::ResourceType_Study:
          publicId = request.orthanc_id_study();
          break;

        case Orthanc::ResourceType_Series:
          publicId = request.orthanc_id_series();
          break;

        case Orthanc::ResourceType_Instance:
          publicId = request.orthanc_id_instance();
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }

      if (publicId.empty())
      {
        throw Orthanc::OrthancException(Orthanc::ErrorCode_InternalError);
      }

      comparisons = " AND " + strTopParentLevel + ".publicId = " + formatter.GenerateParameter(publicId);

      for (int level = queryLevel; level > topParentLevel; level--)
      {
        sql += (" INNER JOIN Resources " +
                FormatLevel(static_cast<Orthanc::ResourceType>(level - 1)) + " ON " +
                FormatLevel(static_cast<Orthanc::ResourceType>(level - 1)) + ".internalId=" +
                FormatLevel(static_cast<Orthanc::ResourceType>(level)) + ".parentId");
      }
    }
    else
    {
      size_t count = 0;

      for (size_t i = 0; i < constraints.GetSize(); i++)
      {
        const DatabaseConstraint& constraint = constraints.GetConstraint(i);

        std::string comparison;

        if (FormatComparison(comparison, formatter, constraint, count, escapeBrackets))
        {
          std::string join;
          FormatJoin(join, constraint, count);
          joins += join;

          if (!comparison.empty())
          {
            comparisons += " AND " + comparison;
          }

          count ++;
        }
      }
    }

    for (int level = queryLevel - 1; level >= upperLevel; level--)
    {
      sql += (" INNER JOIN Resources " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + " ON " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + ".internalId=" +
              FormatLevel(static_cast<Orthanc::ResourceType>(level + 1)) + ".parentId");
    }

    for (int level = queryLevel + 1; level <= lowerLevel; level++)
    {
      sql += (" INNER JOIN Resources " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + " ON " +
              FormatLevel(static_cast<Orthanc::ResourceType>(level - 1)) + ".internalId=" +
              FormatLevel(static_cast<Orthanc::ResourceType>(level)) + ".parentId");
    }

    std::list<std::string> where;
    where.push_back(strQueryLevel + ".resourceType = " +
                    formatter.FormatResourceType(queryLevel) + comparisons);


    if (!request.labels().empty())
    {
      /**
       * "In SQL Server, NOT EXISTS and NOT IN predicates are the best
       * way to search for missing values, as long as both columns in
       * question are NOT NULL."
       * https://explainextended.com/2009/09/15/not-in-vs-not-exists-vs-left-join-is-null-sql-server/
       **/

      std::list<std::string> formattedLabels;
      for (int i = 0; i < request.labels().size(); i++)
      {
        formattedLabels.push_back(formatter.GenerateParameter(request.labels(i)));
      }

      std::string condition;
      switch (request.labels_constraint())
      {
        case Orthanc::DatabasePluginMessages::LABELS_CONSTRAINT_ANY:
          condition = "> 0";
          break;

        case Orthanc::DatabasePluginMessages::LABELS_CONSTRAINT_ALL:
          condition = "= " + boost::lexical_cast<std::string>(request.labels().size());
          break;

        case Orthanc::DatabasePluginMessages::LABELS_CONSTRAINT_NONE:
          condition = "= 0";
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
      }

      where.push_back("(SELECT COUNT(1) FROM Labels AS selectedLabels WHERE selectedLabels.id = " + strQueryLevel +
                      ".internalId AND selectedLabels.label IN (" + Join(formattedLabels, "", ", ") + ")) " + condition);
    }

    sql += joins + Join(where, " WHERE ", " AND ");

    if (request.has_limits())
    {
      sql += formatter.FormatLimits(request.limits().since(), request.limits().count());
    }

  }
#endif


  void ISqlLookupFormatter::ApplySingleLevel(std::string& sql,
                                             ISqlLookupFormatter& formatter,
                                             const DatabaseConstraints& lookup,
                                             Orthanc::ResourceType queryLevel,
                                             const std::set<std::string>& labels,
                                             LabelsConstraint labelsConstraint,
                                             size_t limit
                                             )
  {
    Orthanc::ResourceType lowerLevel, upperLevel;
    GetLookupLevels(lowerLevel, upperLevel, queryLevel, lookup);

    assert(upperLevel == queryLevel &&
           queryLevel == lowerLevel);

    const bool escapeBrackets = formatter.IsEscapeBrackets();

    std::vector<std::string> mainDicomTagsComparisons, dicomIdentifiersComparisons;

    for (size_t i = 0; i < lookup.GetSize(); i++)
    {
      const DatabaseConstraint& constraint = lookup.GetConstraint(i);

      std::string comparison;

      if (FormatComparison2(comparison, formatter, constraint, escapeBrackets))
      {
        if (!comparison.empty())
        {
          if (constraint.IsIdentifier())
          {
            dicomIdentifiersComparisons.push_back(comparison);
          }
          else
          {
            mainDicomTagsComparisons.push_back(comparison);
          }
        }
      }
    }

    sql = ("SELECT publicId, internalId "
           "FROM Resources "
           "WHERE resourceType = " + formatter.FormatResourceType(queryLevel)
            + " ");

    if (dicomIdentifiersComparisons.size() > 0)
    {
      for (std::vector<std::string>::const_iterator it = dicomIdentifiersComparisons.begin(); it < dicomIdentifiersComparisons.end(); ++it)
      {
        sql += (" AND internalId IN (SELECT id FROM DicomIdentifiers WHERE " + *it + ") ");
      }
    }

    if (mainDicomTagsComparisons.size() > 0)
    {
      for (std::vector<std::string>::const_iterator it = mainDicomTagsComparisons.begin(); it < mainDicomTagsComparisons.end(); ++it)
      {
        sql += (" AND internalId IN (SELECT id FROM MainDicomTags WHERE " + *it + ") ");
      }
    }

    if (!labels.empty())
    {
      /**
       * "In SQL Server, NOT EXISTS and NOT IN predicates are the best
       * way to search for missing values, as long as both columns in
       * question are NOT NULL."
       * https://explainextended.com/2009/09/15/not-in-vs-not-exists-vs-left-join-is-null-sql-server/
       **/

      std::list<std::string> formattedLabels;
      for (std::set<std::string>::const_iterator it = labels.begin(); it != labels.end(); ++it)
      {
        formattedLabels.push_back(formatter.GenerateParameter(*it));
      }

      std::string condition;
      std::string inOrNotIn;
      switch (labelsConstraint)
      {
        case LabelsConstraint_Any:
          condition = "> 0";
          inOrNotIn = "IN";
          break;

        case LabelsConstraint_All:
          condition = "= " + boost::lexical_cast<std::string>(labels.size());
          inOrNotIn = "IN";
          break;

        case LabelsConstraint_None:
          condition = "> 0";
          inOrNotIn = "NOT IN";
          break;

        default:
          throw Orthanc::OrthancException(Orthanc::ErrorCode_ParameterOutOfRange);
      }

      sql += (" AND internalId " + inOrNotIn + " (SELECT id"
                                 " FROM (SELECT id, COUNT(1) AS labelsCount "
                                        "FROM Labels "
                                        "WHERE label IN (" + Join(formattedLabels, "", ", ") + ") GROUP BY id"
                                        ") AS temp "
                                 " WHERE labelsCount " + condition + ")");
    }

    if (limit != 0)
    {
      sql += " LIMIT " + boost::lexical_cast<std::string>(limit);
    }
  }
}
