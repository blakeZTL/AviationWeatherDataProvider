using System;
using System.IO;
using System.Net;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;

namespace AviationWeatherDataProvider
{
    public class RetrieveMultiplePlugin : PluginBase
    {
        public RetrieveMultiplePlugin()
            : base(typeof(RetrieveMultiplePlugin))
        {
            // Not implemented
        }

        protected override void ExecuteCdsPlugin(ILocalPluginContext localPluginContext)
        {
            var context = localPluginContext.PluginExecutionContext;
            var sysService = localPluginContext.SystemUserService;
            var tracer = localPluginContext.TracingService;

            EntityCollection entityCollection = new EntityCollection();

            tracer.Trace("Retrieving METARs...");

            try
            {
                var webRequest =
                    WebRequest.Create(
                        "https://aviationweather.gov/api/data/dataserver?requestType=retrieve&dataSource=metars&stationString=%40ga&hoursBeforeNow=1&format=xml&mostRecentForEachStation=constraint"
                    ) as HttpWebRequest;

                if (webRequest == null)
                {
                    return;
                }

                webRequest.ContentType = "xml";

                using (var stream = webRequest.GetResponse().GetResponseStream())
                {
                    using (var streamReader = new StreamReader(stream))
                    {
                        var metarAsXml = streamReader.ReadToEnd();
                        Console.WriteLine(metarAsXml);
                        XmlSerializer serializer = new XmlSerializer(typeof(Response));
                        using (StringReader reader = new StringReader(metarAsXml))
                        {
                            var response = (Response)serializer.Deserialize(reader);
                            entityCollection.Entities.AddRange(
                                response.GetResponseMetars(null, response).Entities
                            );
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                tracer.Trace(ex.ToString());
            }

            context.OutputParameters["BusinessEntityCollection"] = entityCollection;
        }
    }
}
