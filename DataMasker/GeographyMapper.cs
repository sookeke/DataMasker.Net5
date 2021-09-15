using System;
using System.Data;
using Oracle.DataAccess.Client;

namespace DataMasker
{
    public class GeographyMapper : Dapper.SqlMapper.TypeHandler<SdoGeometry>
    {
        public override void SetValue(IDbDataParameter parameter, SdoGeometry value)
        {
            parameter.Value = value == null ? (object)DBNull.Value : value.ToString();
            ((OracleParameter)parameter).UdtTypeName = "MDSYS.SDO_GEOMETRY";
            if (parameter is OracleParameter npgsqlParameter)
            {
                npgsqlParameter.OracleDbType = OracleDbType.Object;
                npgsqlParameter.UdtTypeName = "MDSYS.SDO_GEOMETRY";
                npgsqlParameter.Value = value;
            }
            else
            {
                throw new ArgumentException();
            }
        }
        public override SdoGeometry Parse(object value)
        {
            if (value is SdoGeometry geometry)
            {
                return geometry;
            }

            throw new ArgumentException();
        }
    }
    public class DimElementMapper : Dapper.SqlMapper.TypeHandler<SdoDimArray>
    {
        public override void SetValue(IDbDataParameter parameter, SdoDimArray value)
        {
            parameter.Value = value == null ? (object)DBNull.Value : value.ToString();
            ((OracleParameter)parameter).UdtTypeName = "MDSYS.SDO_DIM_ARRAY";
            if (parameter is OracleParameter npgsqlParameter)
            {
                npgsqlParameter.OracleDbType = OracleDbType.Object;
                npgsqlParameter.UdtTypeName = "MDSYS.SDO_DIM_ARRAY";
                npgsqlParameter.Value = value;
            }
            else
            {
                throw new ArgumentException();
            }
        }
        public override SdoDimArray Parse(object value)
        {
            if (value is SdoDimArray geometry)
            {
                return geometry;
            }

            throw new ArgumentException();
        }
    }
}
