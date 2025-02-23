using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider.QueryConversion
{
    public static class ObservationTimeQueryConversion
    {
        public static string TransformQueryExpression(
            QueryExpression query,
            ITracingService tracer,
            out List<ConditionExpression> unhandeledExpressions
        )
        {
            tracer.Trace(
                $"{nameof(ObservationTimeQueryConversion)}.{nameof(TransformQueryExpression)} Start: {DateTime.Now}"
            );
            unhandeledExpressions = new List<ConditionExpression>();

            string queryParameter = null;
            if (
                query == null
                || (!query.Criteria.Conditions.Any() && !query.Criteria.Filters.Any())
            )
            {
                tracer.Trace("Query is empty.");
                tracer.Trace(
                    $"{nameof(ObservationTimeQueryConversion)}.{nameof(TransformQueryExpression)} End:{DateTime.Now}"
                );
                return queryParameter;
            }

            DateTime startTime = default;
            DateTime endTime = default;
            ProcessConditions(
                query
                    .Criteria.Conditions.Where(c => c.AttributeName == "awx_observationtime")
                    .ToList(),
                unhandeledExpressions
            );
            var filterConditions = query
                .Criteria.Filters.SelectMany(f =>
                    f.Conditions.Where(c => c.AttributeName == "awx_observationtime")
                )
                .ToList();
            ProcessConditions(filterConditions, unhandeledExpressions);

            queryParameter =
                startTime == default
                    ? string.Empty
                    : $"&startTime={startTime:yyyy-MM-ddTHH:mm:ssZ}";

            queryParameter +=
                endTime == default ? string.Empty : $"&endTime={endTime:yyyy-MM-ddTHH:mm:ssZ}";

            tracer.Trace($"Query Parameter: {queryParameter}");
            tracer.Trace($"Unhandeled Expressions: {unhandeledExpressions.Count}");
            tracer.Trace(
                $"{nameof(ObservationTimeQueryConversion)}.{nameof(TransformQueryExpression)} End: {DateTime.Now}"
            );
            return string.IsNullOrEmpty(queryParameter) ? null : queryParameter;

            void ProcessConditions(
                List<ConditionExpression> conditions,
                List<ConditionExpression> expressions
            )
            {
                foreach (var condition in conditions)
                {
                    tracer.Trace($"Condition: {condition.AttributeName} {condition.Operator}");
                    if (condition.AttributeName != "awx_observationtime")
                    {
                        continue;
                    }

                    if (condition.Operator == ConditionOperator.Equal)
                    {
                        var value = (DateTime)condition.Values[0];
                        startTime = value;
                        endTime = value;
                        break;
                    }
                    else if (condition.Operator == ConditionOperator.Between)
                    {
                        var start = (DateTime)condition.Values[0];
                        var end = (DateTime)condition.Values[1];
                        startTime = start;
                        endTime = end;
                        break;
                    }
                    else if (condition.Operator == ConditionOperator.GreaterEqual)
                    {
                        var value = (DateTime)condition.Values[0];
                        startTime = value;
                    }
                    else if (condition.Operator == ConditionOperator.LessEqual)
                    {
                        var value = (DateTime)condition.Values[0];
                        endTime = value;
                    }
                    else if (condition.Operator == ConditionOperator.GreaterThan)
                    {
                        var value = (DateTime)condition.Values[0];
                        startTime = value;
                    }
                    else if (condition.Operator == ConditionOperator.LessThan)
                    {
                        var value = (DateTime)condition.Values[0];
                        endTime = value;
                    }
                    else
                    {
                        tracer.Trace($"Operator {condition.Operator} is unhandeled.");
                        expressions.Add(condition);
                    }
                }
            }
        }
    }
}
