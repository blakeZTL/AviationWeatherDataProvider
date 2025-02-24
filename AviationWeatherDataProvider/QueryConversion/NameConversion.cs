using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider.QueryConversion
{
    public static class NameConversion
    {
        public static string TransformQueryExpression(
            QueryExpression query,
            ITracingService tracer,
            out List<ConditionExpression> unhandeledExpressions
        )
        {
            tracer.Trace(nameof(TransformQueryExpression) + $" Start: {DateTime.Now}");
            unhandeledExpressions = new List<ConditionExpression>();

            string queryParameter = null;
            if (
                query == null
                || (!query.Criteria.Conditions.Any() && !query.Criteria.Filters.Any())
            )
            {
                tracer.Trace("Query is empty.");
                tracer.Trace(nameof(TransformQueryExpression) + $" End: {DateTime.Now}");
                return queryParameter;
            }

            ProcessConditions(
                query.Criteria.Conditions.Where(c => c.AttributeName == "awx_name").ToList(),
                unhandeledExpressions
            );
            var filterConditions = query
                .Criteria.Filters.SelectMany(f =>
                    f.Conditions.Where(c => c.AttributeName == "awx_name")
                )
                .ToList();
            ProcessConditions(filterConditions, unhandeledExpressions);

            void ProcessConditions(
                List<ConditionExpression> conditions,
                List<ConditionExpression> expressions
            )
            {
                foreach (var condition in conditions)
                {
                    if (condition.AttributeName != "awx_name")
                    {
                        continue;
                    }

                    if (condition.Operator == ConditionOperator.Equal)
                    {
                        queryParameter = condition.Values[0] as string;
                        break;
                    }
                    else if (condition.Operator == ConditionOperator.In)
                    {
                        queryParameter = string.Join(
                            ",",
                            condition.Values.Select(v => v as string)
                        );
                        break;
                    }
                    else
                    {
                        tracer.Trace($"Operator {condition.Operator} is unhandeled.");
                        expressions.Add(condition);
                    }
                }
            }
            tracer.Trace($"Unhandeled Expressions: {unhandeledExpressions.Count}");
            tracer.Trace(nameof(TransformQueryExpression) + $" End: {DateTime.Now}");
            return queryParameter;
        }

        public static EntityCollection ProcessUnhandeledExpressions(
            EntityCollection metars,
            List<ConditionExpression> unhandeledExpressions,
            ITracingService tracer
        )
        {
            tracer.Trace(nameof(ProcessUnhandeledExpressions) + $" Start: {DateTime.Now}");

            if (unhandeledExpressions == null || !unhandeledExpressions.Any())
            {
                return metars;
            }

            foreach (var exp in unhandeledExpressions)
            {
                tracer.Trace(
                    $"Processing unhandeled expression: {exp.AttributeName} {exp.Operator} {string.Join(",", exp.Values.Select(v => v as string))}"
                );

                switch (exp.Operator)
                {
                    case ConditionOperator.NotEqual:
                        var filteredEntities = metars.Entities.Where(m =>
                            (string)m["awx_name"] != exp.Values[0] as string
                        );
                        metars.Entities.Clear();
                        metars.Entities.AddRange(filteredEntities);
                        break;
                    case ConditionOperator.Like:
                        var likeValue = exp.Values[0] as string;
                        var regexPattern = "^" + Regex.Escape(likeValue).Replace("%", ".*") + "$";
                        var likeEntities = metars
                            .Entities.Where(m => Regex.IsMatch((string)m["awx_name"], regexPattern))
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
                                !Regex.IsMatch((string)m["awx_name"], notLikePattern)
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notLikeEntities);
                        break;
                    case ConditionOperator.NotIn:
                        var notInEntities = metars
                            .Entities.Where(m =>
                                !exp.Values.Select(v => v as string).Contains((string)m["awx_name"])
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notInEntities);
                        break;
                    case ConditionOperator.Null:
                        var nullEntities = metars
                            .Entities.Where(m =>
                                m.Attributes.ContainsKey("awx_name") && m["awx_name"] == null
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(nullEntities);
                        break;
                    case ConditionOperator.NotNull:
                        var notNullEntities = metars
                            .Entities.Where(m =>
                                m.Attributes.ContainsKey("awx_name") && m["awx_name"] != null
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notNullEntities);
                        break;
                    case ConditionOperator.Contains:
                        var containsValue = exp.Values[0] as string;
                        var containsPattern = ".*" + Regex.Escape(containsValue) + ".*";
                        var containsEntities = metars
                            .Entities.Where(m =>
                                Regex.IsMatch((string)m["awx_name"], containsPattern)
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(containsEntities);
                        break;
                    case ConditionOperator.DoesNotContain:
                        var doesNotContainValue = exp.Values[0] as string;
                        var doesNotContainPattern = ".*" + Regex.Escape(doesNotContainValue) + ".*";
                        var doesNotContainEntities = metars
                            .Entities.Where(m =>
                                !Regex.IsMatch((string)m["awx_name"], doesNotContainPattern)
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(doesNotContainEntities);
                        break;
                    case ConditionOperator.BeginsWith:
                        var beginsWithValue = exp.Values[0] as string;
                        var beginsWithPattern = "^" + Regex.Escape(beginsWithValue);
                        var beginsWithEntities = metars
                            .Entities.Where(m =>
                                Regex.IsMatch((string)m["awx_name"], beginsWithPattern)
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(beginsWithEntities);
                        break;
                    case ConditionOperator.DoesNotBeginWith:
                        var doesNotBeginWithValue = exp.Values[0] as string;
                        var doesNotBeginWithPattern = "^" + Regex.Escape(doesNotBeginWithValue);
                        var doesNotBeginWithEntities = metars
                            .Entities.Where(m =>
                                !Regex.IsMatch((string)m["awx_name"], doesNotBeginWithPattern)
                            )
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(doesNotBeginWithEntities);
                        break;
                    default:
                        tracer.Trace($"Operator {exp.Operator} is not supported.");
                        break;
                }
            }

            tracer.Trace(
                $"Metars count after processing unhandeled expressions: {metars.Entities.Count}"
            );
            tracer.Trace(nameof(ProcessUnhandeledExpressions) + $" End: {DateTime.Now}");
            return metars;
        }
    }
}
