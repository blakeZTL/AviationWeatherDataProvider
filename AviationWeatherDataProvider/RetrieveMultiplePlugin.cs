using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Xml.Serialization;
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

            EntityCollection entityCollection = new EntityCollection();
            tracer.Trace("Retrieving METARs...");

            QueryExpression query = (QueryExpression)context.InputParameters["Query"];
            tracer.Trace("Query: {0}", query.EntityName);
            tracer.Trace("ColumnSet Columns: {0}", string.Join(",", query.ColumnSet.Columns));
            tracer.Trace("Criteria FilterOperator: {0}", query.Criteria.FilterOperator);
            tracer.Trace("Criteria Conditions: {0}", query.Criteria.Conditions.Count);
            string stationIdToFilterFor = null;
            string stationIdToNotReturn = null;
            if (query.Criteria != null && query.Criteria.Conditions.Count != 0)
            {
                foreach (var condition in query.Criteria.Conditions)
                {
                    tracer.Trace("Condition: {0}", condition.AttributeName);
                    tracer.Trace("Operator: {0}", condition.Operator);
                    tracer.Trace("Values: {0}", string.Join(",", condition.Values));
                    if (condition.AttributeName == "awx_name")
                    {
                        if (condition.Operator == ConditionOperator.Equal)
                        {
                            stationIdToFilterFor = condition.Values[0] as string;
                        }
                        else if (condition.Operator == ConditionOperator.NotEqual)
                        {
                            stationIdToNotReturn = condition.Values[0] as string;
                        }
                    }
                }
            }
            if (context.InputParameters.Contains("QueryText"))
            {
                string queryText = context.InputParameters["QueryText"] as string;
                tracer.Trace("QueryText: {0}", queryText);
            }

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
                            if (stationIdToFilterFor != null)
                            {
                                if (metars.Entities.Count > 0)
                                {
                                    var foundEntity = metars.Entities.First(entity =>
                                        entity.GetAttributeValue<string>("awx_name")
                                        == stationIdToFilterFor
                                    );
                                    entityCollection.Entities.Clear();
                                    entityCollection.Entities.Add(foundEntity);
                                    break;
                                }
                            }
                            entityCollection.Entities.AddRange(metars.Entities);
                        }
                    }
                    catch (Exception ex)
                    {
                        tracer.Trace(ex.ToString());
                    }
                }
            }
            if (stationIdToNotReturn != null)
            {
                entityCollection.Entities.Where(entity =>
                    entity.GetAttributeValue<string>("awx_name") != stationIdToNotReturn
                );
            }
            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
        }
    }
}
