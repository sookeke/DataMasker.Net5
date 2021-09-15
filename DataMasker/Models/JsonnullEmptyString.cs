using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;


namespace QuickType
{


    public partial class TopLevel
    {
        [JsonProperty("bar", Required = Required.DisallowNull, NullValueHandling = NullValueHandling.Ignore)]
        public Bar? Bar { get; set; }
    }

    public enum Bar { Foo, Empty };

    public partial class TopLevel
    {
        public static TopLevel FromJson(string json) => JsonConvert.DeserializeObject<TopLevel>(json, QuickType.Converter.Settings);
    }

    static class BarExtensions
    {
        public static Bar? ValueForString(string str)
        {
            Console.WriteLine("value for string");
            switch (str)
            {
                case "": Console.WriteLine("empty"); return Bar.Empty;
                case "foo": return Bar.Foo;
                default: return null;
            }
        }

        public static Bar ReadJson(JsonReader reader, JsonSerializer serializer)
        {
            var str = serializer.Deserialize<string>(reader);
            var maybeValue = ValueForString(str);
            if (maybeValue.HasValue) return maybeValue.Value;
            throw new Exception("Unknown enum case " + str);
        }

        public static void WriteJson(this Bar value, JsonWriter writer, JsonSerializer serializer)
        {
            switch (value)
            {
                case Bar.Empty: serializer.Serialize(writer, ""); break;
                case Bar.Foo: serializer.Serialize(writer, "foo"); break;
            }
        }
    }

    public static class Serialize
    {
        public static string ToJson(this TopLevel self) => JsonConvert.SerializeObject(self, QuickType.Converter.Settings);
    }

    internal class Converter : JsonConverter
    {
        public override bool CanConvert(Type t) => t == typeof(Bar) || t == typeof(Bar?);

        public override object ReadJson(JsonReader reader, Type t, object existingValue, JsonSerializer serializer)
        {
            if (t == typeof(Bar))
                return BarExtensions.ReadJson(reader, serializer);
            if (t == typeof(Bar?))
            {
                if (reader.TokenType == JsonToken.Null) return null;
                return BarExtensions.ReadJson(reader, serializer);
            }
            throw new Exception("Unknown type");
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var t = value.GetType();
            if (t == typeof(Bar))
            {
                ((Bar)value).WriteJson(writer, serializer);
                return;
            }
            throw new Exception("Unknown type");
        }

        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters = {
                new Converter()
            }
        };
    }
}
