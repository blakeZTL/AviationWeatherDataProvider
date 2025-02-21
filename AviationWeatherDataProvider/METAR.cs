using System;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Serialization;
using Microsoft.Xrm.Sdk;

namespace AviationWeatherDataProvider
{
    [XmlRoot(ElementName = "data_source")]
    public class DataSource
    {
        [XmlAttribute(AttributeName = "name")]
        public string Name { get; set; }
    }

    [XmlRoot(ElementName = "request")]
    public class Request
    {
        [XmlAttribute(AttributeName = "type")]
        public string Type { get; set; }
    }

    [XmlRoot(ElementName = "quality_control_flags")]
    public class QualityControlFlags
    {
        [XmlElement(ElementName = "auto_station")]
        public bool AutoStation { get; set; }
    }

    [XmlRoot(ElementName = "sky_condition")]
    public class SkyCondition
    {
        [XmlAttribute(AttributeName = "sky_cover")]
        public string SkyCover { get; set; }

        [XmlAttribute(AttributeName = "cloud_base_ft_agl")]
        public int CloudBaseFtAgl { get; set; }
    }

    [XmlRoot(ElementName = "METAR")]
    public class METAR
    {
        [XmlElement(ElementName = "raw_text")]
        public string RawText { get; set; }

        [XmlElement(ElementName = "station_id")]
        public string StationId { get; set; }

        [XmlElement(ElementName = "observation_time")]
        public DateTime ObservationTime { get; set; }

        [XmlElement(ElementName = "latitude")]
        public double Latitude { get; set; }

        [XmlElement(ElementName = "longitude")]
        public double Longitude { get; set; }

        [XmlElement(ElementName = "temp_c")]
        public decimal TempC { get; set; }

        [XmlElement(ElementName = "dewpoint_c")]
        public double DewpointC { get; set; }

        [XmlElement(ElementName = "wind_dir_degrees")]
        public string WindDirDegrees { get; set; }

        [XmlElement(ElementName = "wind_speed_kt")]
        public int WindSpeedKt { get; set; }

        [XmlElement(ElementName = "visibility_statute_mi")]
        public string VisibilityStatuteMi { get; set; }

        [XmlElement(ElementName = "altim_in_hg")]
        public double AltimInHg { get; set; }

        //[XmlElement(ElementName = "quality_control_flags")]
        //public QualityControlFlags QualityControlFlags { get; set; }

        //[XmlElement(ElementName = "sky_condition")]
        //public SkyCondition SkyCondition { get; set; }

        [XmlElement(ElementName = "flight_category")]
        public string FlightCategory { get; set; }

        [XmlElement(ElementName = "metar_type")]
        public string MetarType { get; set; }

        [XmlElement(ElementName = "elevation_m")]
        public int ElevationM { get; set; }

        public Entity GetMetarAsEntity(ITracingService tracer)
        {
            Entity entity = new Entity("awx_metar");
            var id = Helpers.GenerateGuidFromText(StationId, ObservationTime);
            entity["awx_metarid"] = id;
            entity["awx_rawtext"] = RawText;
            entity["awx_name"] = StationId;
            entity["awx_observationtime"] = ObservationTime;
            entity["awx_latitude"] = Latitude;
            entity["awx_longitude"] = Longitude;
            entity["awx_temp_c"] = TempC;
            entity["awx_dewpoint_c"] = DewpointC;
            entity["awx_wind_dir_degrees"] = WindDirDegrees;
            entity["awx_wind_speed_kt"] = WindSpeedKt;
            entity["awx_visibility_statute_mi"] = VisibilityStatuteMi;
            entity["awx_altim_in_hg"] = AltimInHg;
            ////entity["awx_skycover"] = SkyCondition.SkyCover;
            ////entity["awx_cloudbaseftagl"] = SkyCondition.CloudBaseFtAgl;
            //entity["awx_flightcategory"] = FlightCategory;
            //entity["awx_metartype"] = MetarType;
            //entity["awx_elevationm"] = ElevationM;

            return entity;
        }
    }

    [XmlRoot(ElementName = "data")]
    public class Data
    {
        [XmlElement(ElementName = "METAR")]
        public METAR[] METAR { get; set; }

        [XmlAttribute(AttributeName = "num_results")]
        public int NumResults { get; set; }

        [XmlText]
        public string Text { get; set; }
    }

    [XmlRoot(ElementName = "response")]
    public class Response
    {
        [XmlElement(ElementName = "request_index")]
        public int RequestIndex { get; set; }

        [XmlElement(ElementName = "data_source")]
        public DataSource DataSource { get; set; }

        [XmlElement(ElementName = "request")]
        public Request Request { get; set; }

        [XmlElement(ElementName = "errors")]
        public object Errors { get; set; }

        [XmlElement(ElementName = "warnings")]
        public object Warnings { get; set; }

        [XmlElement(ElementName = "time_taken_ms")]
        public int TimeTakenMs { get; set; }

        [XmlElement(ElementName = "data")]
        public Data Data { get; set; }

        [XmlAttribute(AttributeName = "xsd")]
        public string Xsd { get; set; }

        [XmlAttribute(AttributeName = "xsi")]
        public string Xsi { get; set; }

        [XmlAttribute(AttributeName = "version")]
        public decimal Version { get; set; }

        [XmlAttribute(AttributeName = "noNamespaceSchemaLocation")]
        public string NoNamespaceSchemaLocation { get; set; }

        [XmlText]
        public string Text { get; set; }

        //public Entity GetResponseAsEntity(ITracingService tracer)
        //{
        //    Entity entity = new Entity("awx_metar");
        //    var id = RequestIndex;
        //    var uniqueIdentifier = Helpers.IntToGuid(id);
        //    tracer.Trace("METAR Id: {0} transformed into {1}", id, uniqueIdentifier);

        //    entity["awx_metarid"] = uniqueIdentifier;
        //    entity["awx_metartext"] = Data.METAR.RawText;

        //    return entity;
        //}

        public EntityCollection GetResponseMetars(ITracingService tracer, Response response)
        {
            EntityCollection entityCollection = new EntityCollection() { EntityName = "awx_metar" };
            foreach (var metar in response.Data.METAR)
            {
                var entity = metar.GetMetarAsEntity(tracer);
                entityCollection.Entities.Add(entity);
            }
            return entityCollection;
        }
    }

    static class Helpers
    {
        public static Guid IntToGuid(int value)
        {
            byte[] bytes = new byte[16];
            BitConverter.GetBytes(value).CopyTo(bytes, 0);
            return new Guid(bytes);
        }

        public static int GuidToInt(Guid value)
        {
            byte[] b = value.ToByteArray();
            int bint = BitConverter.ToInt32(b, 0);
            return bint;
        }

        public static Guid GenerateGuidFromText(string stationId, DateTime observationTime)
        {
            // Ensure the stationId is 4-5 characters
            if (stationId.Length < 4 || stationId.Length > 5)
            {
                throw new ArgumentException("StationId must be 4 or 5 characters long.");
            }

            // Format the DateTime to a fixed-length string
            string formattedTime = observationTime.ToString("yyyyMMddHHmm");

            // Combine the StationId and formatted DateTime
            string combinedText = stationId + formattedTime;

            // Ensure the combined length is 16 characters or less
            if (combinedText.Length > 16)
            {
                throw new ArgumentException(
                    $"Combined text {combinedText} is too long to convert to a GUID."
                );
            }

            byte[] textBytes = Encoding.UTF8.GetBytes(combinedText);
            byte[] guidBytes = new byte[16];
            Array.Copy(textBytes, guidBytes, textBytes.Length);
            return new Guid(guidBytes);
        }

        public static (string StationId, DateTime ObservationTime) ParseGuidToText(
            Guid guid,
            ITracingService tracer = null
        )
        {
            byte[] guidBytes = guid.ToByteArray();
            string combinedText = Encoding.UTF8.GetString(guidBytes).TrimEnd('\0');

            // Extract the StationId and DateTime parts
            string stationId = combinedText.Substring(0, 4); // Assuming StationId is always 4 characters
            string dateTimePart = combinedText.Substring(4);

            if (tracer != null)
            {
                tracer.Trace("Parsing GUID to text...");
                tracer.Trace(guid.ToString());
                tracer.Trace(combinedText);
                tracer.Trace(stationId);
                tracer.Trace(dateTimePart);
            }

            DateTime observationTime = DateTime.ParseExact(
                dateTimePart + "00",
                "yyyyMMddHHmmss",
                null
            );
            return (stationId, observationTime);
        }
    }
}
