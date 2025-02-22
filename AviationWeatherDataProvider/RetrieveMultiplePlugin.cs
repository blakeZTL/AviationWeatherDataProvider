using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace AviationWeatherDataProvider
{
    public class RetrieveMultiplePlugin : PluginBase
    {
        //static string _stationString = null;
        //static string[] _antiStationString = { };
        //static string _stationIdBeginsWith = null;

        public RetrieveMultiplePlugin()
            : base(typeof(RetrieveMultiplePlugin)) { }

        protected override void ExecuteCdsPlugin(ILocalPluginContext localPluginContext)
        {
            var context = localPluginContext.PluginExecutionContext;
            var sysService = localPluginContext.SystemUserService;
            var tracer = localPluginContext.TracingService;
            tracer.Trace("Retrieving METARs...");

            QueryExpression query = (QueryExpression)context.InputParameters["Query"];

            (string stationString, string stationIdBeginsWith, string[] antiStationString) =
                ParseMetarNameQuery(query, tracer);
            tracer.Trace("Station String: {0}", stationString);
            tracer.Trace("Station ID Begins With: {0}", stationIdBeginsWith);
            tracer.Trace("Anti-Station String: {0}", string.Join(",", antiStationString));

            EntityCollection entityCollection = new EntityCollection() { EntityName = "awx_metar" };

            if (stationString != null)
            {
                tracer.Trace("Retrieving METARs by station string ({0})...", stationString);
                entityCollection = GetMetarsByStationString(stationString, tracer);
            }
            else
            {
                entityCollection = GetMetarsByStates(tracer);
            }

            if (antiStationString.Length > 0)
            {
                var filteredEntities = entityCollection
                    .Entities.Where(e =>
                        !antiStationString.Contains(e.GetAttributeValue<string>("station_id"))
                    )
                    .ToList();
                entityCollection.Entities.Clear();
                entityCollection.Entities.AddRange(filteredEntities);
            }
            if (stationIdBeginsWith != null)
            {
                var filteredEntities = entityCollection
                    .Entities.Where(e =>
                        e.GetAttributeValue<string>("station_id").StartsWith(stationIdBeginsWith)
                    )
                    .ToList();
                entityCollection.Entities.Clear();
                entityCollection.Entities.AddRange(filteredEntities);
            }

            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
        }

        static (string, string, string[]) ParseMetarNameQuery(
            QueryExpression query,
            ITracingService tracer
        )
        {
            //TODO : Implement Like operator
            string stationString = null;
            string stationIdBeginsWith = null;
            string[] antiStationString = { };
            var nameConditions = query.Criteria.Conditions.Where(q =>
                q.AttributeName == "awx_name"
            );
            tracer.Trace("Name Conditions: {0}", nameConditions.Count());
            foreach (var condition in nameConditions)
            {
                if (stationString != null)
                {
                    continue;
                }
                switch (condition.Operator)
                {
                    case ConditionOperator.Equal:
                        stationString = condition.Values[0] as string;
                        break;
                    case ConditionOperator.NotEqual:
                        antiStationString = condition.Values.Select(v => v.ToString()).ToArray();
                        break;
                    case ConditionOperator.In:
                        stationString = string.Join(
                            ",",
                            condition.Values.Select(v => v.ToString())
                        );
                        break;
                    case ConditionOperator.NotIn:
                        antiStationString = condition.Values.Select(v => v.ToString()).ToArray();
                        break;
                    case ConditionOperator.BeginsWith:
                        stationIdBeginsWith = condition.Values[0] as string;
                        break;

                    default:
                        tracer.Trace("Unsupported operator: {0}", condition.Operator);
                        break;
                }
            }
            var nameFilters = query.Criteria.Filters.Where(q =>
                q.Conditions.Any(c => c.AttributeName == "awx_name")
            );
            tracer.Trace("Name Filters: {0}", nameFilters.Count());
            foreach (var filter in nameFilters)
            {
                foreach (var condition in filter.Conditions)
                {
                    if (stationString != null)
                    {
                        continue;
                    }
                    switch (condition.Operator)
                    {
                        case ConditionOperator.Equal:
                            stationString = condition.Values[0] as string;
                            break;
                        case ConditionOperator.NotEqual:
                            antiStationString = condition
                                .Values.Select(v => v.ToString())
                                .ToArray();
                            break;
                        case ConditionOperator.In:
                            stationString = string.Join(
                                ",",
                                condition.Values.Select(v => v.ToString())
                            );
                            break;
                        case ConditionOperator.NotIn:
                            antiStationString = condition
                                .Values.Select(v => v.ToString())
                                .ToArray();
                            break;
                        case ConditionOperator.BeginsWith:
                            stationIdBeginsWith = condition.Values[0] as string;
                            break;
                        default:
                            tracer.Trace("Unsupported operator: {0}", condition.Operator);
                            break;
                    }
                }
            }

            return (stationString, stationIdBeginsWith, antiStationString);
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
