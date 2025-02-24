using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using AviationWeatherDataProvider.Models;
using AviationWeatherDataProvider.QueryConversion;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider
{
    public class RetrieveMultiplePlugin : PluginBase
    {
        public RetrieveMultiplePlugin()
            : base(typeof(RetrieveMultiplePlugin)) { }

        protected override void ExecuteCdsPlugin(ILocalPluginContext localPluginContext)
        {
            var context = localPluginContext.PluginExecutionContext;
            var _ = localPluginContext.SystemUserService;
            var tracer = localPluginContext.TracingService;

            QueryExpression query = (QueryExpression)context.InputParameters["Query"];

            string stationString = NameConversion.TransformQueryExpression(
                query,
                tracer,
                out var unhandeledNameExpressions
            );

            string observationTimeQuery = ObservationTimeQueryConversion.TransformQueryExpression(
                query,
                tracer,
                out var _
            );

            EntityCollection entityCollection;
            var filteredEntities = new EntityCollection();
            try
            {
                if (stationString != null)
                {
                    entityCollection = GetMetarsByStationString(
                        stationString,
                        tracer,
                        observationTimeQuery
                    );
                }
                else
                {
                    entityCollection = GetMetarsByStates(tracer);
                }

                if (unhandeledNameExpressions.Any())
                {
                    entityCollection = NameConversion.ProcessUnhandeledExpressions(
                        entityCollection,
                        unhandeledNameExpressions,
                        tracer
                    );
                }

                if (
                    !query.Criteria.Filters.Any()
                    || !query.Criteria.Filters.Any(f => f.Conditions.Count > 0)
                )
                {
                    tracer.Trace("No filters in query");
                    tracer.Trace("Conditions found: " + query.Criteria.Conditions.Count.ToString());
                    if (query.Criteria.Conditions.Any())
                    {
                        var filter = new FilterExpression
                        {
                            FilterOperator = query.Criteria.FilterOperator
                        };
                        filter.Conditions.AddRange(query.Criteria.Conditions);
                        query.Criteria.Filters.Add(filter);
                    }
                }

                foreach (var filter in query.Criteria.Filters)
                {
                    tracer.Trace($"Filter conditions: {filter.Conditions.Count}");
                    if (filter.FilterOperator == LogicalOperator.And)
                    {
                        var intersectingEntities = new List<Entity>();
                        var filteredByVis = new EntityCollection();
                        var filteredByWxString = new EntityCollection();
                        tracer.Trace("Filter operator is AND");
                        foreach (var condition in filter.Conditions)
                        {
                            tracer.Trace(
                                "Attribute: {0}, Operator: {1}, Value: {2}",
                                condition.AttributeName,
                                condition.Operator,
                                condition.Values?.Count > 0 ? condition.Values[0] : "null"
                            );
                            try
                            {
                                var results = VisibilityQueryConversion.GetFilteredCollection(
                                    entityCollection,
                                    condition,
                                    tracer
                                );
                                if (filteredByVis.Entities.Count == 0)
                                {
                                    tracer.Trace("Visibility count: " + results.Entities.Count);
                                    filteredByVis.Entities.AddRange(results.Entities);
                                }
                                else
                                {
                                    tracer.Trace(
                                        "Visibility count before revision: "
                                            + results.Entities.Count
                                    );
                                    var revisedEntities = results.Entities.Intersect(
                                        filteredByVis.Entities
                                    );
                                    filteredByVis.Entities.Clear();
                                    filteredByVis.Entities.AddRange(revisedEntities);

                                    tracer.Trace(
                                        $"Visibility count after revision: {filteredByVis.Entities.Count}"
                                    );
                                }
                            }
                            catch (Exception ex)
                            {
                                tracer.Trace(ex.ToString());
                                throw new InvalidPluginExecutionException(
                                    "Error filtering METARs by visibility."
                                );
                            }

                            try
                            {
                                var results = WeatherStringQueryConversion.GetFilteredCollection(
                                    entityCollection,
                                    condition,
                                    tracer
                                );
                                if (filteredByWxString.Entities.Count == 0)
                                {
                                    filteredByWxString.Entities.AddRange(results.Entities);
                                }
                                else
                                {
                                    var revisedEntities = results.Entities.Where(
                                        e => filteredByWxString.Entities.Contains(e)
                                    );
                                    filteredByWxString.Entities.Clear();
                                    filteredByWxString.Entities.AddRange(revisedEntities);
                                }
                                tracer.Trace(
                                    $"Weather string count: {filteredByWxString.Entities.Count}"
                                );
                            }
                            catch (Exception ex)
                            {
                                tracer.Trace(ex.ToString());
                                throw new InvalidPluginExecutionException(
                                    "Error filtering METARs by weather string."
                                );
                            }
                        }
                        if (filteredByWxString.Entities.Any() && filteredByVis.Entities.Any())
                        {
                            tracer.Trace("Both visibility and weather string filters");
                            intersectingEntities.AddRange(
                                entityCollection.Entities
                                    .Intersect(filteredByWxString.Entities)
                                    .Intersect(filteredByVis.Entities)
                                    .ToList()
                            );
                        }
                        else if (filteredByWxString.Entities.Any() && !filteredByVis.Entities.Any())
                        {
                            tracer.Trace("Only weather string filter");
                            intersectingEntities.AddRange(
                                entityCollection.Entities
                                    .Intersect(filteredByWxString.Entities)
                                    .ToList()
                            );
                        }
                        else if (!filteredByWxString.Entities.Any() && filteredByVis.Entities.Any())
                        {
                            tracer.Trace("Only visibility filter");
                            intersectingEntities.AddRange(
                                entityCollection.Entities.Intersect(filteredByVis.Entities).ToList()
                            );
                        }
                        else
                        {
                            tracer.Trace("No entities to apply intersection logic with");
                        }
                        filteredEntities.Entities.AddRange(intersectingEntities);
                    }
                    else if (filter.FilterOperator == LogicalOperator.Or)
                    {
                        var unionEntities = new List<Entity>();
                        foreach (var condition in filter.Conditions)
                        {
                            tracer.Trace("OR filter operator");
                            EntityCollection filteredByVis;
                            try
                            {
                                filteredByVis = VisibilityQueryConversion.GetFilteredCollection(
                                    entityCollection,
                                    condition,
                                    tracer
                                );
                                tracer.Trace($"Visibility count: {filteredByVis.Entities.Count}");
                            }
                            catch (Exception ex)
                            {
                                tracer.Trace(ex.ToString());
                                throw new InvalidPluginExecutionException(
                                    "Error filtering METARs by visibility."
                                );
                            }
                            EntityCollection filteredByWxString;
                            try
                            {
                                filteredByWxString =
                                    WeatherStringQueryConversion.GetFilteredCollection(
                                        entityCollection,
                                        condition,
                                        tracer
                                    );
                                tracer.Trace(
                                    $"Weather string count: {filteredByWxString.Entities.Count}"
                                );
                            }
                            catch (Exception ex)
                            {
                                tracer.Trace(ex.ToString());
                                throw new InvalidPluginExecutionException(
                                    "Error filtering METARs by weather string."
                                );
                            }
                            try
                            {
                                // add filteredVis entities to unionEntities that arent already there
                                unionEntities.AddRange(
                                    filteredByVis.Entities
                                        .Where(entity => !unionEntities.Contains(entity))
                                        .ToList()
                                );
                                // add filteredWxString entities to unionEntities that arent already there
                                unionEntities.AddRange(
                                    filteredByWxString.Entities
                                        .Where(entity => !unionEntities.Contains(entity))
                                        .ToList()
                                );
                                tracer.Trace($"Union count: {unionEntities.Count}");
                            }
                            catch (Exception ex)
                            {
                                tracer.Trace(ex.ToString());
                                throw new InvalidPluginExecutionException(
                                    "Error filtering METARs by visibility or weather string."
                                );
                            }
                        }
                        filteredEntities.Entities.AddRange(
                            entityCollection.Entities.Intersect(unionEntities).ToList()
                        );
                    }
                }
                // TODO: Handle filter logical operators for groupings of conditions with different logical operators
            }
            catch (Exception ex)
            {
                tracer.Trace(ex.ToString());
                throw new InvalidPluginExecutionException(ex.Message);
            }

            context.OutputParameters["BusinessEntityCollection"] = filteredEntities;
        }

        static EntityCollection GetMetarsByStationString(
            string stationString,
            ITracingService tracer,
            string observationTimeQuery = null
        )
        {
            EntityCollection entities = new EntityCollection() { EntityName = "awx_metar" };
            var uri =
                $"https://aviationweather.gov/api/data/metar?ids={stationString}&format=json&taf=true";
            if (observationTimeQuery != null)
            {
                uri += observationTimeQuery;
            }
            tracer.Trace(nameof(GetMetarsByStationString) + " " + uri);
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    if (!(WebRequest.Create(uri) is HttpWebRequest webRequest))
                    {
                        return entities;
                    }
                    webRequest.ContentType = "application/json";
                    using (var stream = webRequest.GetResponse().GetResponseStream())
                    {
                        using (var streamReader = new StreamReader(stream))
                        {
                            var metarAsJson = streamReader.ReadToEnd();
                            var metarModels = JsonSerializer.Deserialize<List<MetarModel.Metar>>(
                                metarAsJson
                            );
                            foreach (var metar in metarModels)
                            {
                                var entity = metar.ToEntity(tracer);
                                entities.Entities.Add(entity);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    tracer.Trace(ex.ToString());
                    throw new InvalidPluginExecutionException("Error retrieving METARs.");
                }
            }

            return entities;
        }

        static EntityCollection GetMetarsByStates(ITracingService tracer)
        {
            EntityCollection entities = new EntityCollection() { EntityName = "awx_metar" };
            using (HttpClient client = new HttpClient())
            {
                var stateBatches = States.Initials
                    .Select((state, index) => new { state, index })
                    .GroupBy(x => x.index / 10)
                    .Select(g => g.Select(x => x.state).ToArray())
                    .ToArray();

                foreach (var batch in stateBatches)
                {
                    var stationString = string.Join(
                        ",",
                        batch.Select(state => $"%40{state.ToLower()}")
                    );

                    try
                    {
                        if (
                            !(
                                WebRequest.Create(
                                    $"https://aviationweather.gov/api/data/metar?ids={stationString}&format=json&taf=true"
                                )
                                is HttpWebRequest webRequest
                            )
                        )
                        {
                            return entities;
                        }
                        webRequest.ContentType = "application/json";
                        using (var stream = webRequest.GetResponse().GetResponseStream())
                        {
                            using (var streamReader = new StreamReader(stream))
                            {
                                var metarAsJson = streamReader.ReadToEnd();
                                var metarModels = JsonSerializer.Deserialize<
                                    List<MetarModel.Metar>
                                >(metarAsJson);
                                foreach (var metar in metarModels)
                                {
                                    var entity = metar.ToEntity(tracer);
                                    entities.Entities.Add(entity);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        tracer.Trace(ex.ToString());
                        throw new InvalidPluginExecutionException("Error retrieving METARs.");
                    }
                }
            }

            return entities;
        }
    }
}
