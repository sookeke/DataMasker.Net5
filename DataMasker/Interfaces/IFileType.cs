using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.Interfaces
{
    interface IFileType
    {
        object GeneratePDF(string path, string table);
        object GenerateTXT(string path, string table);
        object GenerateRTF(string path, string table);
        object GenerateDOCX(string path, string table);
        object GenerateJPEG(string path, string table);
        object GenerateMSG(string path, string table);
        object GenerateHTML(string path, string table);
        object GenerateXLSX(string path, string table);
        object GenerateTIF(string path, string table);
        object GenerateTIFF(string path, string table);
        object GenerateRandom(string path);
    }
}
