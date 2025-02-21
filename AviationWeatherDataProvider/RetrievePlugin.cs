using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;

namespace AviationWeatherDataProvider
{
    public class RetrievePlugin : PluginBase
    {
        public RetrievePlugin()
            : base(typeof(RetrievePlugin)) { }

        protected override void ExecuteCdsPlugin(ILocalPluginContext localPluginContext)
        {
            var context = localPluginContext.PluginExecutionContext;
            var sysService = localPluginContext.SystemUserService;
            var tracer = localPluginContext.TracingService;

            Entity entity = null;

            tracer.Trace("Retrieving METAR...");

            try
            {
                var guid = context.PrimaryEntityId;
                tracer.Trace($"Primary Entity ID: {guid}");

                var stationTime = Helpers.ParseGuidToText(guid, tracer);
                tracer.Trace($"Station Id: {stationTime.StationId}");
                tracer.Trace($"Time: {stationTime.ObservationTime}");

                var dateString = ConvertDateTimeToString(stationTime.ObservationTime);
                var stationId = stationTime.StationId;

                var webRequest =
                    WebRequest.Create(
                        $"https://aviationweather.gov/api/data/metar?ids={stationId}&format=xml&date={dateString}"
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
                        tracer.Trace(metarAsXml);
                        XmlSerializer serializer = new XmlSerializer(typeof(Response));
                        using (StringReader reader = new StringReader(metarAsXml))
                        {
                            var response = (Response)serializer.Deserialize(reader);
                            var metars = response.GetResponseMetars(tracer, response);
                            entity = metars.Entities.FirstOrDefault();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                tracer.Trace(ex.ToString());
                entity = null;
            }

            if (entity != null)
            {
                context.OutputParameters["BusinessEntity"] = entity;
            }
        }

        public static string ConvertDateTimeToString(DateTime dateTime)
        {
            // Format the date into the desired string format
            string formattedDateString = dateTime.ToString("yyyyMMdd_HHmmss") + "Z";

            return formattedDateString;
        }
    }
}
