using NetTopologySuite.IO.Sdo;
using NetTopologySuite.IO.UdtBase;
using Oracle.DataAccess.Types;

namespace DataMasker
{
    [OracleCustomTypeMappingAttribute("MDSYS.SDO_GEOMETRY")]
    public class SdoGeometry: OracleCustomTypeBase<SdoGeometry>
    {
        private enum OracleObjectColumns { SDO_GTYPE, SDO_SRID, SDO_POINT, SDO_ELEM_INFO, SDO_ORDINATES, SDO_DIM_ARRAY }
        private decimal? sdo_Gtype;
        [OracleObjectMappingAttribute(0)]
        public decimal? Sdo_Gtype
        {
            get { return sdo_Gtype; }
            set { sdo_Gtype = value; }
        }
        private decimal? sdo_Srid;
        
        [OracleObjectMappingAttribute(1)]
        public decimal? Sdo_Srid
        {
            get { return sdo_Srid; }
            set { sdo_Srid = value; }
        }
        private SdoPoint point;
        [Oracle.DataAccess.Types.OracleObjectMappingAttribute(2)]
        public SdoPoint Point
        {
            get { return point; }
            set { point = value; }
        }
        private decimal[] elemArray;
        [Oracle.DataAccess.Types.OracleObjectMappingAttribute(3)]
        public decimal[] ElemArray
        {
            get { return elemArray; }
            set { elemArray = value; }
        }
        private decimal[] ordinatesArray;
        [Oracle.DataAccess.Types.OracleObjectMappingAttribute(4)]
        public decimal[] OrdinatesArray
        {
            get { return ordinatesArray; }
            set { ordinatesArray = value; }
        }
      
        [OracleCustomTypeMappingAttribute("MDSYS.SDO_ELEM_INFO_ARRAY")]
        public class ElemArrayFactory : OracleArrayTypeFactoryBase<decimal> { }
        [Oracle.DataAccess.Types.OracleCustomTypeMappingAttribute("MDSYS.SDO_ORDINATE_ARRAY")]
        public class OrdinatesArrayFactory : OracleArrayTypeFactoryBase<decimal> { }
      
        public override void MapFromCustomObject()
        {
            SetValue((int)OracleObjectColumns.SDO_GTYPE, Sdo_Gtype);
            SetValue((int)OracleObjectColumns.SDO_SRID, Sdo_Srid);
            SetValue((int)OracleObjectColumns.SDO_POINT, Point);
            SetValue((int)OracleObjectColumns.SDO_ELEM_INFO, ElemArray);
            SetValue((int)OracleObjectColumns.SDO_ORDINATES, OrdinatesArray);
           // SetValue((int)OracleObjectColumns.SDO_DIM_ARRAY, DimArray);
        }
        public override void MapToCustomObject()
        {
            Sdo_Gtype = GetValue<decimal?>((int)OracleObjectColumns.SDO_GTYPE);
            Sdo_Srid = GetValue<decimal?>((int)OracleObjectColumns.SDO_SRID);
            Point = GetValue<SdoPoint>((int)OracleObjectColumns.SDO_POINT);
            ElemArray = GetValue<decimal[]>((int)OracleObjectColumns.SDO_ELEM_INFO);
            OrdinatesArray = GetValue<decimal[]>((int)OracleObjectColumns.SDO_ORDINATES);
            //DimArray = GetValue<decimal[]>((int)OracleObjectColumns.SDO_DIM_ARRAY);
        }
    }
}
