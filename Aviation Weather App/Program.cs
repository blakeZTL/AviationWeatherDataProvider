using System.Net;
using System.Xml.Serialization;
using AviationWeatherDataProvider;
using Microsoft.Xrm.Sdk;

EntityCollection entityCollection = new EntityCollection();
try
{
    var webRequest = (HttpWebRequest)
        WebRequest.Create(
            "https://aviationweather.gov/api/data/dataserver?requestType=retrieve&dataSource=metars&stationString=%40ga&hoursBeforeNow=1&format=xml&mostRecentForEachStation=constraint"
        );

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
            XmlSerializer serializer = new(typeof(Response));
            using (StringReader reader = new(metarAsXml))
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
    Console.WriteLine(ex.ToString());
}

foreach (var entity in entityCollection.Entities)
{
    Console.WriteLine(entity["awx_metarid"]);
    Console.WriteLine(entity["awx_rawtext"]);
    Console.WriteLine(entity["awx_stationid"]);
    Console.WriteLine(entity["awx_observationtime"]);
}
