using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;

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
                        tracer.Trace("metarXml: {0}", metarAsXml);
                        XmlSerializer serializer = new XmlSerializer(typeof(Response));
                        using (StringReader reader = new StringReader(metarAsXml))
                        {
                            var responseObj = (Response)serializer.Deserialize(reader);
                            entityCollection.Entities.AddRange(
                                responseObj.GetResponseMetars(null, responseObj).Entities
                            );
                        }
                    }
                    catch (Exception ex)
                    {
                        tracer.Trace(ex.ToString());
                    }
                }
            }
            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
        }
    }
}
