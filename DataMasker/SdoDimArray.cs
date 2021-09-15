using NetTopologySuite.IO.Sdo;
using NetTopologySuite.IO.UdtBase;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System;

namespace DataMasker
{
    [OracleCustomTypeMappingAttribute("MDSYS.SDO_DIM_ARRAY")]
    public class SdoDimArray : CustomCollectionTypeBase<SdoDimArray, SdoDimElement>{ }
    public abstract class CustomCollectionTypeBase<TType, TValue> : CustomTypeBase<TType>, IOracleArrayTypeFactory where TType : CustomTypeBase<TType>, new()
    {
        private enum OracleObjectColumns { SDO_DIM_ELEMENT }
        [OracleArrayMapping()]
        public TValue[] Values;
        public override void FromCustomObject(OracleConnection connection, IntPtr pointerUdt)
        {
            OracleUdt.SetValue(connection, pointerUdt, 0, Values);
        }

        public override void ToCustomObject(OracleConnection connection, IntPtr pointerUdt)
        {
            Values = (TValue[])OracleUdt.GetValue(connection, pointerUdt, 0);
        }

        public Array CreateArray(int numElems)
        {
            return new TValue[numElems];
        }

        public Array CreateStatusArray(int numElems)
        {
            return new OracleUdtStatus[numElems];
        }
      
    }
    public abstract class CustomTypeBase<T> : IOracleCustomType, IOracleCustomTypeFactory, INullable where T : CustomTypeBase<T>, new()
    {
        private bool _isNull;

        public IOracleCustomType CreateObject()
        {
            return new T();
        }

        public abstract void FromCustomObject(OracleConnection connection, IntPtr pointerUdt);

        public abstract void ToCustomObject(OracleConnection connection, IntPtr pointerUdt);

        public bool IsNull
        {
            get { return this._isNull; }
        }

        public static T Null
        {
            get { return new T { _isNull = true }; }
        }
    }
}
