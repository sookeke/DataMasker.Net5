using DataMasker.Interfaces;
using MsgKit;
using MsgKit.Enums;
using PdfSharp.Drawing;
using System;
using System.IO;
using System.Linq;
using Bogus;
using MessageImportance = MsgKit.Enums.MessageImportance;
using GemBox.Spreadsheet;
using SautinSoft.Document;
using PaperType = SautinSoft.Document.PaperType;
using PdfSaveOptions = SautinSoft.Document.PdfSaveOptions;
using Color1 = System.Drawing.Color;
using System.Drawing;
using Color = SautinSoft.Document.Color;
using System.Drawing.Imaging;
using WaffleGenerator;
using System.Configuration;
using PdfSharp.Pdf;

namespace DataMasker
{
    public class FileType : IFileType
    {
        Faker faker = new Faker();
        public object GenerateDOCX(string docPath, string text)
        {
            // Let's create a simple document.
            var rant = WaffleEngine.Text(rnd, 2, false);
            DocumentCore dc = new DocumentCore();

            // Add new section.
            Section section = new Section(dc);
            dc.Sections.Add(section);

            // Let's set page size A4.
            section.PageSetup.PaperType = PaperType.A4;

            // Add two paragraphs using different ways:
            // Way 1: Add 1st paragraph.
            Paragraph par1 = new Paragraph(dc);
            par1.ParagraphFormat.Alignment = HorizontalAlignment.Center;
            section.Blocks.Add(par1);

            // Let's create a characterformat for text in the 1st paragraph.
            CharacterFormat cf = new CharacterFormat() { FontName = "Verdana", Size = 16, FontColor = Color.Orange };

            Run text1 = new Run(dc, "Ministry of Transportation and Infrastructure")
            {
                CharacterFormat = cf
            };
            par1.Inlines.Add(text1);

            // Let's add a line break into our paragraph.
            par1.Inlines.Add(new SpecialCharacter(dc, SpecialCharacterType.LineBreak));

            Run text2 = text1.Clone();
            text2.Text = ConfigurationManager.AppSettings["DatabaseName"];
            par1.Inlines.Add(text2);

            // Way 2 (easy): Add 2nd paragraph using ContentRange.
            dc.Content.End.Insert(rant, new CharacterFormat() { Size = 25, FontColor = Color.Blue, Bold = true });
            SpecialCharacter lBr = new SpecialCharacter(dc, SpecialCharacterType.LineBreak);
            dc.Content.End.Insert(lBr.Content);
            dc.Content.End.Insert("Signed: Business SMEs.", new CharacterFormat() { Size = 20, FontColor = Color.DarkGreen, UnderlineStyle = UnderlineType.Single });

            // Save a document to a file into DOCX format.
            dc.Save(docPath, new DocxSaveOptions());
            if (File.Exists(docPath))
            {
                return docPath;
            }
            return null;
        }

        public object GenerateHTML(string htmlFixedPath, string text)
        {
            DocumentCore dc = new DocumentCore();
            var rant = WaffleEngine.Text(rnd, 2, false);
            // Add new section.
            Section section = new Section(dc);
            dc.Sections.Add(section);
            text = rant;

            // Add two paragraphs using different ways:
            // Way 1: Add 1st paragraph.
            Paragraph par1 = new Paragraph(dc);
            par1.ParagraphFormat.Alignment = HorizontalAlignment.Center;
            section.Blocks.Add(par1);

            // Let's create a characterformat for text in the 1st paragraph.
            CharacterFormat cf = new CharacterFormat() { FontName = "Verdana", Size = 16, FontColor = Color.Orange };

            Run text1 = new Run(dc, "Ministry of Transportation and Infrastructure")
            {
                CharacterFormat = cf
            };
            par1.Inlines.Add(text1);

            // Let's add a line break into our paragraph.
            par1.Inlines.Add(new SpecialCharacter(dc, SpecialCharacterType.LineBreak));

            Run text2 = text1.Clone();
            text2.Text = "Property Information Management System";
            par1.Inlines.Add(text2);

            // Way 2 (easy): Add 2nd paragraph using ContentRange.
            dc.Content.End.Insert(text, new CharacterFormat() { Size = 25, FontColor = Color.Blue, Bold = true });
            SpecialCharacter lBr = new SpecialCharacter(dc, SpecialCharacterType.LineBreak);
            dc.Content.End.Insert(lBr.Content);
            dc.Content.End.Insert("Signed: Business SMEs", new CharacterFormat() { Size = 20, FontColor = Color.DarkGreen, UnderlineStyle = UnderlineType.Single });

            // Save HTML document as HTML-fixed format.
            dc.Save(htmlFixedPath, new HtmlFixedSaveOptions());
            if (File.Exists(htmlFixedPath))
            {
                return htmlFixedPath;

            }
            return null;

        }
        private static ImageCodecInfo GetEncoderInfo(String mimeType)
        {
            int j;
            ImageCodecInfo[] encoders;
            encoders = ImageCodecInfo.GetImageEncoders();
            for (j = 0; j < encoders.Length; ++j)
            {
                if (encoders[j].MimeType == mimeType)
                    return encoders[j];
            }
            return null;
        }

        public object GenerateJPEG(string path, string table)
        {
            Bitmap myBitmap;
            ImageCodecInfo myImageCodecInfo;
            System.Drawing.Imaging.Encoder myEncoder;
            EncoderParameter myEncoderParameter;
            EncoderParameters myEncoderParameters;

            // Create a Bitmap object based on a BMP file.
            myBitmap = new Bitmap(100,400);

            // Get an ImageCodecInfo object that represents the JPEG codec.
            myImageCodecInfo = GetEncoderInfo("image/jpeg");

            // Create an Encoder object based on the GUID

            // for the Quality parameter category.
            myEncoder = System.Drawing.Imaging.Encoder.Quality;

            // Create an EncoderParameters object.

            // An EncoderParameters object has an array of EncoderParameter

            // objects. In this case, there is only one

            // EncoderParameter object in the array.
            myEncoderParameters = new EncoderParameters(1);

            // Save the bitmap as a JPEG file with quality level 25.
            myEncoderParameter = new EncoderParameter(myEncoder, 25L);
            myEncoderParameters.Param[0] = myEncoderParameter;
            myBitmap.Save("Shapes025.jpg", myImageCodecInfo, myEncoderParameters);

            // Save the bitmap as a JPEG file with quality level 50.
            myEncoderParameter = new EncoderParameter(myEncoder, 50L);
            myEncoderParameters.Param[0] = myEncoderParameter;
            myBitmap.Save(path, myImageCodecInfo, myEncoderParameters);

            // Save the bitmap as a JPEG file with quality level 75.
            myEncoderParameter = new EncoderParameter(myEncoder, 75L);
            myEncoderParameters.Param[0] = myEncoderParameter;
            myBitmap.Save(path, myImageCodecInfo, myEncoderParameters);
            if (File.Exists(path))
            {
                return path;
            }
            return null;
        }

        public object GenerateMSG(string path, string table)
        {
            var rant = WaffleEngine.Text(rnd, 2, false);
            using (var email = new Email(
          new Sender("peterpan@neverland.com", "Peter Pan"),
          new Representing("tinkerbell@neverland.com", "Tinkerbell"),
          "Hello Neverland subject"))
            {
                email.Recipients.AddTo("captainhook@neverland.com", "Captain Hook");
                email.Recipients.AddCc("crocodile@neverland.com", "The evil ticking crocodile");
                email.Subject = "Property Information Management Systems";
                email.BodyText = "Property Information Management Systems";
                email.BodyHtml = "<html><head></head><body>MOTI: Welcome to the Property Information Management System "+ Environment.NewLine + rant + "  </body></html>";
                email.Importance = MessageImportance.IMPORTANCE_HIGH;
                email.IconIndex = MessageIconIndex.ReadMail;
                //email.Attachments.Add(@"d:\crocodile.jpg");
                email.Save(Environment.CurrentDirectory + path);

                // Show the E-mail
                //System.Diagnostics.Process.Start(path);
                if (File.Exists(Environment.CurrentDirectory + path))
                {
                    return Environment.CurrentDirectory + path;
                }
                return null;
            }
        }

        public object GeneratePDF(string pdfPath, string table)
        {
            //PdfDocument pdf = new PdfDocument();
            //PdfPage pdfPage = pdf.AddPage();
            var rant = WaffleEngine.Text(rnd, 3, false);


            //XGraphics graph = XGraphics.FromPdfPage(pdfPage);

            //XFont font = new XFont("Verdana", 12, XFontStyle.Regular);
            //graph.DrawString("Property Information Management System: " + Environment.NewLine  + faker.Rant.Review(), font, XBrushes.Black,
            //new XRect(0, 0, pdfPage.Width.Point, pdfPage.Height.Point), XStringFormats.TopLeft
            //);
            //graph.DrawString(faker.Rant.Review(), font, XBrushes.Black,
            //new XRect(0, 0, pdfPage.Width.Point, pdfPage.Height.Point), XStringFormats.TopLeft);
            // graph.DrawString()

            DocumentCore dc = new DocumentCore();

            // Add new section.
            Section section = new Section(dc);
            dc.Sections.Add(section);

            // Let's set page size A4.
            section.PageSetup.PaperType = PaperType.A4;

            // Add two paragraphs using different ways:

            // Way 1: Add 1st paragraph.
            Paragraph par1 = new Paragraph(dc);
            par1.ParagraphFormat.Alignment = HorizontalAlignment.Center;
            section.Blocks.Add(par1);

            // Let's create a characterformat for text in the 1st paragraph.
            CharacterFormat cf = new CharacterFormat() { FontName = "Verdana", Size = 16, FontColor = Color.Orange };

            Run text1 = new Run(dc, "Ministry of Transportation and Infrastructure!")
            {
                CharacterFormat = cf
            };
            par1.Inlines.Add(text1);

            // Let's add a line break into our paragraph.
            par1.Inlines.Add(new SpecialCharacter(dc, SpecialCharacterType.LineBreak));

            Run text2 = text1.Clone();
            text2.Text = "Information Management Branch.";// + Environment.NewLine;
            par1.Inlines.Add(text2);
            //par1.Inlines.Add(Environment.NewLine)

            // Way 2 (easy): Add 2nd paragraph using ContentRange.
            dc.Content.End.Insert(rant, new CharacterFormat() { Size = 25, FontColor = Color.Blue, Bold = true });
            SpecialCharacter lBr = new SpecialCharacter(dc, SpecialCharacterType.LineBreak);
            dc.Content.End.Insert(lBr.Content);
            dc.Content.End.Insert("Signed: Business SMEs.", new CharacterFormat() { Size = 20, FontColor = Color.DarkGreen, UnderlineStyle = UnderlineType.Single });

            // Save PDF to a file
            dc.Save(Environment.CurrentDirectory + pdfPath, new PdfSaveOptions());


            //pdf.Save(Environment.CurrentDirectory+path);
            if (File.Exists(Environment.CurrentDirectory+ pdfPath))
            {
                return Environment.CurrentDirectory+ pdfPath;
            }
            return null ;
        }

        public object GenerateTIF(string path, string table)
        {
            return MakeRandomImage(path);
        }
        public object GenerateRTF(string rtfPath, string text)
        {
            // Let's create a simple Rtf document.
            DocumentCore dc = new DocumentCore();
            var rant = WaffleEngine.Text(rnd, 2, false);
            // Add new section.
            Section section = new Section(dc);
            dc.Sections.Add(section);

            // Let's set page size A4.
            section.PageSetup.PaperType = PaperType.A4;

            // Add two paragraphs using different ways:
            // Way 1: Add 1st paragraph.
            Paragraph par1 = new Paragraph(dc);
            par1.ParagraphFormat.Alignment = HorizontalAlignment.Center;
            section.Blocks.Add(par1);

            // Let's create a characterformat for text in the 1st paragraph.
            CharacterFormat cf = new CharacterFormat() { FontName = "Verdana", Size = 16, FontColor = Color.Orange };

            Run text1 = new Run(dc, "Ministry of Transportation and Infrastructure")
            {
                CharacterFormat = cf
            };
            par1.Inlines.Add(text1);

            // Let's add a line break into our paragraph.
            par1.Inlines.Add(new SpecialCharacter(dc, SpecialCharacterType.LineBreak));

            Run text2 = text1.Clone();
            text2.Text = "Information Management Branch";
            par1.Inlines.Add(text2);

            // Way 2 (easy): Add 2nd paragraph using ContentRange.
            dc.Content.End.Insert(WaffleEngine.Text(rnd, 4, false), new CharacterFormat() { Size = 25, FontColor = Color.Blue, Bold = true });
            SpecialCharacter lBr = new SpecialCharacter(dc, SpecialCharacterType.LineBreak);
            dc.Content.End.Insert(lBr.Content);
            dc.Content.End.Insert("Signed: Business SMEs", new CharacterFormat() { Size = 20, FontColor = Color.DarkGreen, UnderlineStyle = UnderlineType.Single });

            // Save Rtf to a file
            dc.Save(rtfPath, new RtfSaveOptions());
            if (File.Exists(rtfPath))
            {
                return rtfPath;
            }
            return null;
        }

        public object GenerateTXT(string path, string table)
        {
            //var reviews = faker.Rant.re
            File.WriteAllText(path, Environment.NewLine+ string.Join("", faker.Rant.Reviews("Product",40).ToArray()));
            if (File.Exists(path))
            {
                return path;
            }
            return null;
        }

        public object GenerateXLSX(string path, string desc)
        {
            SpreadsheetInfo.SetLicense("FREE-LIMITED-KEY");
            Faker faker = new Faker();
            var Worksheet = new ExcelFile();
            //Config
            var worksheet = Worksheet.Worksheets.Add(desc);
            worksheet.Cells["A1"].Value = string.Join(" ", faker.Rant.Reviews(" ", 10).ToArray()).ToString();
            Worksheet.Save(path);
            if (File.Exists(path))
            {
                return path;
            }
            return null;
        }
        public object GenerateRandom(string path)
        {
            PdfDocument pdf = new PdfDocument();
            PdfPage pdfPage = pdf.AddPage();


            XGraphics graph = XGraphics.FromPdfPage(pdfPage);

            XFont font = new XFont("arial", 12, XFontStyle.Bold);
            graph.DrawString("Property Information Management System: " + Environment.NewLine + faker.Rant.Review(), font, XBrushes.Black,
            new XRect(0, 0, pdfPage.Width.Point, pdfPage.Height.Point), XStringFormats.TopLeft
            );
            pdf.Save(path);
            if (File.Exists(path))
            {
                return  path;
            }
            return null;
        }


        private static Random rnd = new Random();
        private static string MakeRandomImage(string imagePath)
        {
            System.Drawing.Image img = new System.Drawing.Bitmap(400, 400, System.Drawing.Imaging.PixelFormat.Format32bppRgb);

            using (System.Drawing.Graphics g = System.Drawing.Graphics.FromImage(img))
            {
                g.Clear(Color1.White);
                for (int i = 0; i < 42; i++)
                {
                    Pen p = GetRandomPen();
                    Point start = GetRandomPoint();
                    Point end = GetRandomPoint();
                    g.DrawLine(p, start, end);
                    g.Save();
                }
            }
            var imgStream = new MemoryStream();
            if (Path.GetExtension(imagePath).ToUpper() == ".TIFF")
            {

                img.Save(imgStream, System.Drawing.Imaging.ImageFormat.Tiff);
                img.Save(imagePath, System.Drawing.Imaging.ImageFormat.Tiff);
                imgStream.Position = 0;
            }
            else
            {
                
                img.Save(imgStream, System.Drawing.Imaging.ImageFormat.Png);
                img.Save(imagePath, System.Drawing.Imaging.ImageFormat.Png);
                imgStream.Position = 0;
            }
            if (File.Exists(imagePath))
            {
                return imagePath;
            }
            return string.Empty;
        }
        /// Generates a random Pen
        private static System.Drawing.Pen GetRandomPen()
        {
            Int32 LineWidth = rnd.Next(3, 25);
            Color1 rndColor = Color1.FromArgb(rnd.Next(256), rnd.Next(256), rnd.Next(256));
            return new Pen(rndColor, LineWidth);
        }

        /// Gets a random point
        private static Point GetRandomPoint()
        {
            int start = rnd.Next(0, 390);
            int end = rnd.Next(0, 390);
            return new Point(start, end);
        }

        public object GenerateTIFF(string path, string table)
        {
            throw new NotImplementedException();
        }
    }
}
