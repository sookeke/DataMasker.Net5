using System;
using NetTopologySuite.IO.UdtBase;
using Oracle.DataAccess.Types;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker
{
    [OracleCustomTypeMappingAttribute("MDSYS.SDO_DIM_ELEMENT")]
    public class SdoDimElement: OracleCustomTypeBase<SdoDimElement>
    {
        private enum OracleObjectColumns { SDO_DIMNAME, SDO_LB, SDO_UB, SDO_TOLERANCE }
        private string _dimname;
        private decimal? _lb;
        private decimal? _ub;
        private decimal? _tolerance;
        [OracleObjectMapping("SDO_DIMNAME")]
        public string SDO_DIMNAME
        {
            get { return _dimname; }
            set { _dimname = value; }
        }
        [OracleObjectMapping("SDO_LB")]
        public decimal? LB
        {
            get { return _lb; }
            set { _lb = value; }
        }
        [OracleObjectMapping("SDO_UB")]
        public decimal? UB
        {
            get { return _ub; }
            set { _ub = value; }
        }
        [OracleObjectMapping("SDO_TOLERANCE")]
        public decimal? TOLERANCE
        {
            get { return _tolerance; }
            set { _tolerance = value; }
        }

        public override void MapFromCustomObject()
        {
            SetValue((int)OracleObjectColumns.SDO_DIMNAME, _dimname);
            SetValue((int)OracleObjectColumns.SDO_LB, _lb);
            SetValue((int)OracleObjectColumns.SDO_UB, _ub);
            SetValue((int)OracleObjectColumns.SDO_TOLERANCE, _tolerance);
        }
        public override void MapToCustomObject()
        {
            SDO_DIMNAME = GetValue<string>((int)OracleObjectColumns.SDO_DIMNAME);
            LB = GetValue<decimal?>((int)OracleObjectColumns.SDO_LB);
            UB = GetValue<decimal?>((int)OracleObjectColumns.SDO_UB);
            TOLERANCE = GetValue<decimal?>((int)OracleObjectColumns.SDO_TOLERANCE);
        }
       
    }
}
