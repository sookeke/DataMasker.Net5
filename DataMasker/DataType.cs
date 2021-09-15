namespace DataMasker
{
    /// <summary>
    /// 
    /// </summary>
    public enum DataType
    {
        /// <summary>
        /// The none
        /// </summary>
        None,
        CompanyPersonName,
        Company,
        NULL,
        PostalCode,
        StringConcat,
        Date,
        Shuffle,
        Ignore,
        Money,
        Location,
        NoMasking,
        math,
        Shufflegeometry,
        Unmask,
        ShufflePolygon,
        exception,
        Vehicle,

        /// <summary>
        /// The api data type, supports {{entity.property}} e.g. {{address.FullAddress}}
        /// </summary>
        Bogus,

        RandomUsername,
        /// <summary>
        /// The first name
        /// </summary>
        FirstName,

        /// <summary>
        /// The last name
        /// </summary>
        LastName,

        /// <summary>
        /// The date of birth
        /// </summary>
        DateOfBirth,

        PickRandom,
        RandomString2,

        /// <summary>
        /// The rant
        /// </summary>
        Rant,
        State,
        City,

        /// <summary>
        /// The lorem
        /// </summary>
        Lorem,
        TimeSpan,

        /// <summary>
        /// The string format
        /// </summary>
        StringFormat,

        /// <summary>
        /// The full address
        /// </summary>
        FullAddress,
        SecondaryAddress,
        StreetAddress,

        /// <summary>
        /// The phone number
        /// </summary>
        PhoneNumber,
        PhoneNumberInt,
        Longitude,
        Latitude,
        RandomSeason,
        FullName,
        File,
        Filename,
        Blob,
        Clob,
        RandomDec,
        //Polygon with same precision
        Geometry,
        RandomYear,
        RandomMonth,
        

        RandomInt,
        Randomint64,

        /// <summary>
        /// Indicates that the column value is computed from other indicated columns
        /// </summary>
        Computed,
        Scramble,
        MaskingOut,

        RandomHexa,
        Error
    }
    public static class DataTypeextension
    {
        public const string None = "What";
        public const string CompanyPersonName = "the";
        public static string ToFriendlyString(this DataType me)
        {
           
            switch (me)
            {
                case DataType.None:
                    return None;
                case DataType.CompanyPersonName:
                    return "CompanyPersonName";
                case DataType.Company:
                    return "Company";
                case DataType.NULL:
                    return "NULL";
                case DataType.PostalCode:
                    return "PostalCode";
                case DataType.StringConcat:
                    return "StringConcat";
                case DataType.Shuffle:
                    return "Shuffle";
                case DataType.Ignore:
                    return "Ignore";
                case DataType.Money:
                    return "Money";
                case DataType.Location:
                    return "Location";
                case DataType.NoMasking:
                    return "NoMasking";
                case DataType.math:
                    return "math";
                case DataType.Shufflegeometry:
                    return "Shufflegeometry";
                case DataType.exception:
                    return "exception";
                case DataType.Bogus:
                    return "Bogus";
                case DataType.RandomUsername:
                    return "RandomUsername";
                case DataType.FirstName:
                    return "FirstName";
                case DataType.LastName:
                    return "LastName";
                case DataType.DateOfBirth:
                    return "DateOfBirth";
                case DataType.PickRandom:
                    return "PickRandom";
                case DataType.RandomString2:
                    return "RandomString2";
                case DataType.Rant:
                    return "Rant";
                case DataType.State:
                    return "State";
                case DataType.City:
                    return "City";
                case DataType.Lorem:
                    return "Lorem";
                case DataType.StringFormat:
                    return "StringFormat";
                case DataType.FullAddress:
                    return "FullAddress";
                case DataType.PhoneNumber:
                    return "PhoneNumber";
                case DataType.PhoneNumberInt:
                    return "PhoneNumberInt";
                case DataType.Longitude:
                    return "Longitude";
                case DataType.Latitude:
                    return "Latitude";
                case DataType.RandomSeason:
                    return "RandomSeason";
                case DataType.File:
                    return "File";
                case DataType.Filename:
                    return "Filename";
                case DataType.Blob:
                    return "Blob";
                case DataType.Clob:
                    return "Clob";
                case DataType.RandomDec:
                    return "RandomDec";
                case DataType.Geometry:
                    return "Geometry";
                case DataType.RandomYear:
                    return "RandomYear";
                case DataType.RandomInt:
                    return "RandomInt";
                case DataType.Computed:
                    return "Computed";
                case DataType.RandomHexa:
                    return "RandomHexa";
                default:
                    return "NULL";
            }
         
        }
    }
}
