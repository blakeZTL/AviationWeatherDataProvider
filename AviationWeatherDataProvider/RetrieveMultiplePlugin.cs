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
            var sysService = localPluginContext.SystemUserService;
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
                // TODO: Handle filter logical operators for groupings of conditions with different logical operators
                entityCollection = VisibilityQueryConversion.ProcessUnhandeledExpressions(
                    entityCollection,
                    query,
                    tracer
                );
                entityCollection = WeatherStringQueryConversion.ProcessUnhandeledExpressions(
                    entityCollection,
                    query,
                    tracer
                );
                entityCollection = CloudsQueryConversion.ProcessUnhandeledExpressions(
                    entityCollection,
                    query,
                    tracer
                );
            }
            catch (Exception ex)
            {
                tracer.Trace(ex.ToString());
                throw new InvalidPluginExecutionException(ex.Message);
            }

            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
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
                var stateBatches = States
                    .Initials.Select((state, index) => new { state, index })
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
