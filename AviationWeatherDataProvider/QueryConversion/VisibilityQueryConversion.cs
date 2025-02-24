using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider.QueryConversion
{
    public static class VisibilityQueryConversion
    {
        private static readonly string attributeName = "awx_visibility";

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
            var conditions = new List<ConditionExpression>();
            conditions.AddRange(
                query.Criteria.Conditions.Where(c => c.AttributeName == attributeName).ToList()
            );
            conditions.AddRange(
                query.Criteria.Filters.SelectMany(
                    f => f.Conditions.Where(c => c.AttributeName == attributeName)
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
                        var equalEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                //tracer.Trace(
                                //    $"Comparing for Equal: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                                //);
                                return entityValue == conditionValue;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(equalEntities);
                        break;
                    case ConditionOperator.NotEqual:
                        var notEqualEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                //tracer.Trace(
                                //    $"Comparing for NotEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                                //);
                                return entityValue != conditionValue;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(notEqualEntities);
                        break;
                    case ConditionOperator.Like:
                        var likeValue = exp.Values[0] as string;
                        var regexPattern = "^" + Regex.Escape(likeValue).Replace("%", ".*") + "$";
                        var likeEntities = metars.Entities
                            .Where(m =>
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
                        var notLikeEntities = metars.Entities
                            .Where(m =>
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
                        var inEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValues = exp.Values.Select(v => (decimal)v);
                                var isIn = conditionValues.Contains(entityValue);
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
                        var notInEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValues = exp.Values.Select(v => (decimal)v);
                                var isNotIn = !conditionValues.Contains(entityValue);
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
                        var nullEntities = metars.Entities
                            .Where(m =>
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
                        var notNullEntities = metars.Entities
                            .Where(m =>
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
                    case ConditionOperator.GreaterThan:
                        var greaterThanEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                var isGreaterThan = entityValue > conditionValue;

                                //tracer.Trace(
                                //    $"Comparing for GreaterThan: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsGreaterThan = {isGreaterThan}"
                                //);
                                return isGreaterThan;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(greaterThanEntities);
                        break;
                    case ConditionOperator.LessThan:
                        var lessThanEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                var isLessThan = entityValue < conditionValue;

                                //tracer.Trace(
                                //    $"Comparing for LessThan: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsLessThan = {isLessThan}"
                                //);
                                return isLessThan;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(lessThanEntities);
                        break;

                    case ConditionOperator.GreaterEqual:
                        var greaterEqualEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                var isGreaterEqual = entityValue >= conditionValue;

                                //tracer.Trace(
                                //    $"Comparing for GreaterEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsGreaterEqual = {isGreaterEqual}"
                                //);
                                return isGreaterEqual;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(greaterEqualEntities);
                        break;
                    case ConditionOperator.LessEqual:
                        var lessEqualEntities = metars.Entities
                            .Where(m =>
                            {
                                var entityValue = m.GetAttributeValue<decimal>(attributeName);
                                var conditionValue = (decimal)exp.Values[0];
                                var isLessEqual = entityValue <= conditionValue;

                                //tracer.Trace(
                                //    $"Comparing for LessEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsLessEqual = {isLessEqual}"
                                //);
                                return isLessEqual;
                            })
                            .ToList();
                        metars.Entities.Clear();
                        metars.Entities.AddRange(lessEqualEntities);
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

        public static EntityCollection GetFilteredCollection(
            EntityCollection metars,
            ConditionExpression condition,
            ITracingService tracer
        )
        {
            tracer.Trace(
                nameof(VisibilityQueryConversion)
                    + "."
                    + nameof(GetFilteredCollection)
                    + $" Start: {DateTime.Now}"
            );
            EntityCollection filteredMetars = new EntityCollection();
            if (condition.AttributeName != attributeName)
            {
                tracer.Trace(
                    $"Condition Attribute Name {condition.AttributeName} is not equal to {attributeName}"
                );
                return filteredMetars;
            }
            switch (condition.Operator)
            {
                case ConditionOperator.Equal:
                    var equalEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValue = (decimal)condition.Values?[0];
                            //tracer.Trace(
                            //    $"Comparing for Equal: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                            //);
                            return entityValue == conditionValue;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(equalEntities);
                    break;
                case ConditionOperator.NotEqual:
                    var notEqualEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValue = (decimal)condition.Values?[0];
                            //tracer.Trace(
                            //    $"Comparing for NotEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}"
                            //);
                            return entityValue != conditionValue;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(notEqualEntities);
                    break;
                case ConditionOperator.Like:
                    var likeValue = condition.Values?[0] as string;
                    var regexPattern = "^" + Regex.Escape(likeValue).Replace("%", ".*") + "$";
                    var likeEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<string>(attributeName);
                            var isMatch = Regex.IsMatch(entityValue, regexPattern);
                            //tracer.Trace(
                            //    $"Comparing for Like: Entity Value = {entityValue}, Pattern = {regexPattern}, IsMatch = {isMatch}"
                            //);
                            return isMatch;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(likeEntities);
                    break;
                case ConditionOperator.NotLike:
                    var notLikeValue = condition.Values?[0] as string;
                    var notLikePattern = "^" + Regex.Escape(notLikeValue).Replace("%", ".*") + "$";
                    var notLikeEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<string>(attributeName);
                            var isMatch = !Regex.IsMatch(entityValue, notLikePattern);
                            //tracer.Trace(
                            //    $"Comparing for NotLike: Entity Value = {entityValue}, Pattern = {notLikePattern}, IsMatch = {isMatch}"
                            //);
                            return isMatch;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(notLikeEntities);
                    break;
                case ConditionOperator.In:
                    var inEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValues = condition.Values.Select(v => (decimal)v);
                            var isIn = conditionValues.Contains(entityValue);
                            //tracer.Trace(
                            //    $"Comparing for In: Entity Value = {entityValue}, Condition Values = {string.Join(",", conditionValues)}, IsIn = {isIn}"
                            //);
                            return isIn;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(inEntities);
                    break;
                case ConditionOperator.NotIn:
                    var notInEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValues = condition.Values.Select(v => (decimal)v);
                            var isNotIn = !conditionValues.Contains(entityValue);
                            //tracer.Trace(
                            //    $"Comparing for NotIn: Entity Value = {entityValue}, Condition Values = {string.Join(",", conditionValues)}, IsNotIn = {isNotIn}"
                            //);
                            return isNotIn;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(notInEntities);
                    break;

                case ConditionOperator.Null:
                    var nullEntities = metars.Entities
                        .Where(m =>
                        {
                            var isNull =
                                m.Attributes.ContainsKey(attributeName) && m[attributeName] == null;
                            //tracer.Trace(
                            //    $"Comparing for Null: Entity Value = {m.GetAttributeValue<string>(attributeName)}, IsNull = {isNull}"
                            //);
                            return isNull;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(nullEntities);
                    break;
                case ConditionOperator.NotNull:
                    var notNullEntities = metars.Entities
                        .Where(m =>
                        {
                            if (!m.Attributes.ContainsKey(attributeName))
                                return false;
                            var attributeValue = m.GetAttributeValue<decimal?>(attributeName);
                            var isNotNull = attributeValue.HasValue;

                            //if (!isNotNull)
                            //    tracer.Trace(
                            //        $"Comparing for NotNull: Entity Value = {m.GetAttributeValue<decimal>(attributeName)}, IsNotNull = {isNotNull}"
                            //    );
                            return isNotNull;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(notNullEntities);
                    break;
                case ConditionOperator.GreaterThan:
                    var greaterThanEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValue = (decimal)condition.Values?[0];
                            var isGreaterThan = entityValue > conditionValue;

                            //tracer.Trace(
                            //    $"Comparing for GreaterThan: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsGreaterThan = {isGreaterThan}"
                            //);
                            return isGreaterThan;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(greaterThanEntities);
                    break;
                case ConditionOperator.LessThan:
                    var lessThanEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValue = (decimal)condition.Values?[0];
                            var isLessThan = entityValue < conditionValue;

                            //tracer.Trace(
                            //    $"Comparing for LessThan: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsLessThan = {isLessThan}"
                            //);
                            return isLessThan;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(lessThanEntities);
                    break;

                case ConditionOperator.GreaterEqual:
                    var greaterEqualEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal>(attributeName);
                            var conditionValue = (decimal)condition.Values?[0];
                            var isGreaterEqual = entityValue >= conditionValue;

                            //tracer.Trace(
                            //    $"Comparing for GreaterEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsGreaterEqual = {isGreaterEqual}"
                            //);
                            return isGreaterEqual;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(greaterEqualEntities);
                    break;
                case ConditionOperator.LessEqual:
                    var lessEqualEntities = metars.Entities
                        .Where(m =>
                        {
                            var entityValue = m.GetAttributeValue<decimal?>(attributeName);
                            if (!entityValue.HasValue)
                                return false;
                            var conditionValue = (decimal)condition.Values?[0];
                            var isLessEqual = entityValue <= conditionValue;

                            //tracer.Trace(
                            //    $"Comparing for LessEqual: Entity Value = {entityValue}, Condition Value = {conditionValue}, IsLessEqual = {isLessEqual}"
                            //);
                            return isLessEqual;
                        })
                        .ToList();
                    filteredMetars.Entities.AddRange(lessEqualEntities);
                    break;

                default:
                    tracer.Trace($"Unhandeled expression: {condition.Operator}");
                    break;
            }
            tracer.Trace(
                nameof(VisibilityQueryConversion)
                    + "."
                    + nameof(GetFilteredCollection)
                    + $" End: {DateTime.Now}"
            );
            return filteredMetars;
        }
    }
}
