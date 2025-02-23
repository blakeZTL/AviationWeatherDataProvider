using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Xml.Serialization;
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

            EntityCollection entityCollection;

            if (stationString != null)
            {
                tracer.Trace("Retrieving METARs by station string ({0})...", stationString);
                entityCollection = GetMetarsByStationString(stationString, tracer);
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

            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
        }

        static EntityCollection GetMetarsByStationString(
            string stationString,
            ITracingService tracer
        )
        {
            EntityCollection entities = new EntityCollection() { EntityName = "awx_metar" };
            using (HttpClient client = new HttpClient())
            {
                try
                {
                    var response = client
                        .GetAsync(
                            $"https://aviationweather.gov/api/data/dataserver?requestType=retrieve&dataSource=metars&stationString={stationString}&hoursBeforeNow=1&format=xml&mostRecentForEachStation=constraint"
                        )
                        .Result;

                    response.EnsureSuccessStatusCode();

                    var metarAsXml = response.Content.ReadAsStringAsync().Result;
                    XmlSerializer serializer = new XmlSerializer(typeof(Response));
                    using (StringReader reader = new StringReader(metarAsXml))
                    {
                        var responseObj = (Response)serializer.Deserialize(reader);
                        EntityCollection metars = responseObj.GetResponseMetars(null, responseObj);

                        entities.Entities.AddRange(metars.Entities);
                    }
                }
                catch (Exception ex)
                {
                    tracer.Trace(ex.ToString());
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
                        var response = client
                            .GetAsync(
                                $"https://aviationweather.gov/api/data/dataserver?requestType=retrieve&dataSource=metars&stationString={stationString}&hoursBeforeNow=1&format=xml&mostRecentForEachStation=constraint"
                            )
                            .Result;

                        response.EnsureSuccessStatusCode();

                        var metarAsXml = response.Content.ReadAsStringAsync().Result;
                        XmlSerializer serializer = new XmlSerializer(typeof(Response));
                        using (StringReader reader = new StringReader(metarAsXml))
                        {
                            var responseObj = (Response)serializer.Deserialize(reader);
                            if (responseObj.Data.NumResults == 1000)
                            {
                                tracer.Trace("Too many results returned. Skipping batch.");
                                throw new InvalidPluginExecutionException(
                                    $"Too many results returned for {stationString}"
                                );
                            }
                            EntityCollection metars = responseObj.GetResponseMetars(
                                null,
                                responseObj
                            );

                            entities.Entities.AddRange(metars.Entities);
                        }
                    }
                    catch (Exception ex)
                    {
                        tracer.Trace(ex.ToString());
                    }
                }
            }

            return entities;
        }
    }
}
