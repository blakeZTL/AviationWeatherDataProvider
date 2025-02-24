﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider.QueryConversion
{
    public static class VisibilityQueryConversion
    {
        public static EntityCollection ProcessUnhandeledExpressions(
            EntityCollection metars,
            QueryExpression query,
            ITracingService tracer
        )
        {
            tracer.Trace(
                nameof(VisibilityQueryConversion)
                    + "."
                    + nameof(ProcessUnhandeledExpressions)
                    + $" Start: {DateTime.Now}"
            );
            var attributeName = "awx_visibility_statute_mi";
            var conditions = new List<ConditionExpression>();
            conditions.AddRange(
                query.Criteria.Conditions.Where(c => c.AttributeName == attributeName).ToList()
            );
            conditions.AddRange(
                query.Criteria.Filters.SelectMany(f =>
                    f.Conditions.Where(c => c.AttributeName == attributeName)
                )
            );

            foreach (var exp in conditions)
            {
                tracer.Trace(
                    $"Processing unhandeled expression: {exp.AttributeName} {exp.Operator} {string.Join(",", exp.Values.Select(v => v as string))}"
                );

                switch (exp.Operator)
                {
                    case ConditionOperator.Equal:
                        var equalEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName)
                                    ?.Trim();
                                var conditionValue = (exp.Values[0] as string)?.Trim();
                                //tracer.Trace(
                                //    $"Comparing for Equal: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                                //);
                                return string.Equals(
                                    entityValue,
                                    conditionValue,
                                    StringComparison.OrdinalIgnoreCase
                                );
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(equalEntities);
                        break;
                    case ConditionOperator.NotEqual:
                        var notEqualEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName)
                                    ?.Trim();
                                var conditionValue = (exp.Values[0] as string)?.Trim();
                                //tracer.Trace(
                                //    $"Comparing for NotEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                                //);
                                return !string.Equals(
                                    entityValue,
                                    conditionValue,
                                    StringComparison.OrdinalIgnoreCase
                                );
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notEqualEntities);
                        break;
                    case ConditionOperator.Like:
                        var likeValue = exp.Values[0] as string;
                        var regexPattern = "^" + Regex.Escape(likeValue).Replace("%", ".*") + "$";
                        var likeEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName);
                                var isMatch = Regex.IsMatch(entityValue, regexPattern);
                                //tracer.Trace(
                                //    $"Comparing for Like: Entity Value = {entityValue}, Pattern = {regexPattern}, IsMatch = {isMatch}"
                                //);
                                return isMatch;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(likeEntities);
                        break;
                    case ConditionOperator.NotLike:
                        var notLikeValue = exp.Values[0] as string;
                        var notLikePattern =
                            "^" + Regex.Escape(notLikeValue).Replace("%", ".*") + "$";
                        var notLikeEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName);
                                var isMatch = !Regex.IsMatch(entityValue, notLikePattern);
                                //tracer.Trace(
                                //    $"Comparing for NotLike: Entity Value = {entityValue}, Pattern = {notLikePattern}, IsMatch = {isMatch}"
                                //);
                                return isMatch;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notLikeEntities);
                        break;
                    case ConditionOperator.In:
                        var inEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName)
                                    ?.Trim();
                                var conditionValues = exp.Values.Select(v => (v as string)?.Trim());
                                var isIn = conditionValues.Contains(
                                    entityValue,
                                    StringComparer.OrdinalIgnoreCase
                                );
                                //tracer.Trace(
                                //    $"Comparing for In: Entity Value = {entityValue}, Condition Values = {string.Join(",", conditionValues)}, IsIn = {isIn}"
                                //);
                                return isIn;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(inEntities);
                        break;
                    case ConditionOperator.NotIn:
                        var notInEntities = metars
                            .Entities.Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<string>(attributeName)
                                    ?.Trim();
                                var conditionValues = exp.Values.Select(v => (v as string)?.Trim());
                                var isNotIn = !conditionValues.Contains(
                                    entityValue,
                                    StringComparer.OrdinalIgnoreCase
                                );
                                //tracer.Trace(
                                //    $"Comparing for NotIn: Entity Value = {entityValue}, Condition Values = {string.Join(",", conditionValues)}, IsNotIn = {isNotIn}"
                                //);
                                return isNotIn;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notInEntities);
                        break;

                    case ConditionOperator.Null:
                        var nullEntities = metars
                            .Entities.Where(m =>
                            {
                                var isNull =
                                    m.Attributes.ContainsKey(attributeName)
                                    && m[attributeName] == null;
                                //tracer.Trace(
                                //    $"Comparing for Null: Entity Value = {m.GetAttributeValue<string>(attributeName)}, IsNull = {isNull}"
                                //);
                                return isNull;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(nullEntities);
                        break;
                    case ConditionOperator.NotNull:
                        var notNullEntities = metars
                            .Entities.Where(m =>
                            {
                                var isNotNull =
                                    m.Attributes.ContainsKey(attributeName)
                                    && m[attributeName] != null;
                                //tracer.Trace(
                                //    $"Comparing for NotNull: Entity Value = {m.GetAttributeValue<string>(attributeName)}, IsNotNull = {isNotNull}"
                                //);
                                return isNotNull;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notNullEntities);
                        break;
                    default:
                        tracer.Trace($"Unhandeled expression: {exp.Operator}");
                        break;
                }
            }

            tracer.Trace(
                $"Metars count after processing unhandeled expressions: {metars.Entities.Count}"
            );
            tracer.Trace(
                nameof(VisibilityQueryConversion)
                    + "."
                    + nameof(ProcessUnhandeledExpressions)
                    + $" End: {DateTime.Now}"
            );
            return metars;
        }
    }
}
