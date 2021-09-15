using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.DataLang
{
   public class RawDataPrd
    {
        public List<IDictionary<string, object>> rows;
        public RawDataPrd(Dictionary<string, object> PrdData)
        {
            rows.Add(PrdData);
           

        }
        public IEnumerable<IDictionary<string, object>> Rows

        {

            get { return rows; }

           

        }

       
    }
}