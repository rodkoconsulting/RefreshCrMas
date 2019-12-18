using System;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using CrystalDecisions.CrystalReports.Engine;
using CrystalDecisions.Shared;

namespace RefreshCrMas2017v2
{
    class RefreshCrMas2017v2
    {
        private const string SsqlConnection = "Server=POL-MAS-02;Database=POL;Integrated Security=True";
        private const string ReportDirectory = "\\\\POL-MAS-02\\Sage\\540\\MAS90\\Reports\\";
        private const string ReportDirectoryTemp = "\\\\POL-MAS-02\\Sage\\540\\MAS90\\Reports\\temp\\";
        private static string _reportFile;
        private static string _parameterName;
        private static string _parameterField;
        private static string _parameterFieldName;
        private static string[] _parameterFieldArray; 
        private static Boolean _parameterAll;
        private static string _parameterTable;
        static ParameterDiscreteValue _discreteParam;
        static ParameterValues _defaultValues;
        private static SqlDataReader _oReaderRefresh;
        private static SqlCommand _oCommandRefresh;
        private static SqlDataReader _oReaderParams;
        private static SqlCommand _oCommandParams;
        private static SqlConnection _conMas;
        private static SqlConnection _conParams;
        private static ReportDocument _rptDocument;

        private static Boolean IsReportMissing { get { return (_reportFile.Length == 0); }  }
    


        public static void SendEmailLog(string sMessage)
        {
            var conLog = new SqlConnection(SsqlConnection);
            conLog.Open();
            var comLog = new SqlCommand("Email_Refresh_Message", conLog) { CommandType = CommandType.StoredProcedure };
            comLog.Parameters.AddWithValue("@Message", sMessage);
            comLog.ExecuteNonQuery();
            comLog.Dispose();
            conLog.Close();
        }

        public static void AddValue(string sValue)
        {
            _discreteParam = new ParameterDiscreteValue { Value = sValue };
            _defaultValues.Add(_discreteParam);
        }

        public static void AddValue(string sValue, string sDescription)
        {
            _discreteParam = new ParameterDiscreteValue { Value = sValue, Description = sDescription};
            _defaultValues.Add(_discreteParam);
        }

        private static void LoadReport()
        {
            try
            {
                _rptDocument = new ReportDocument();
                _rptDocument.Load(ReportDirectory + _reportFile);
                _defaultValues = new ParameterValues();
                _rptDocument.DataDefinition.ParameterFields[_parameterName].ApplyDefaultValues(_defaultValues);
            }
            catch (Exception ex)
            {
                SendEmailLog("Error opening Crystal Report " + _reportFile + ": " + ex.Message);
                if (_rptDocument != null)
                    _rptDocument.Dispose();
                if (_oReaderRefresh != null)
                    _oReaderRefresh.Close();
                if (_oCommandRefresh != null)
                    _oCommandRefresh.Dispose();
                if (_conMas != null)
                    _conMas.Close();
             }
        }

        private static void LoadParameters()
        {
            try
            {
                _conParams = new SqlConnection(SsqlConnection);
                _conParams.Open();
                _oCommandParams = new SqlCommand
                {
                    Connection = _conParams,
                    CommandText = (_parameterFieldArray.Length == 1)
                        ? "SELECT DISTINCT " + _parameterField + " FROM " + _parameterTable + " WHERE (" +
                          _parameterField + " IS NOT NULL AND LEN(" + _parameterField + ") > 0) ORDER BY " + _parameterField
                        : "SELECT DISTINCT " + _parameterField + " FROM " + _parameterTable + " ORDER BY " +
                          _parameterField
                };
                if (_parameterAll)
                    AddValue("*");
                _oReaderParams = _oCommandParams.ExecuteReader();
                while (_oReaderParams.Read())
                {
                    if (_parameterFieldArray.Length == 1)
                    {
                        if (_oReaderParams[_parameterFieldName].ToString().Substring(0, 1) != "/")
                            AddValue(_oReaderParams[_parameterFieldName].ToString());
                    }
                    else
                    {
                        if (_oReaderParams[_parameterFieldArray[1]].Equals(DBNull.Value))
                            AddValue(_oReaderParams[_parameterFieldArray[0]].ToString());
                        else
                            AddValue(_oReaderParams[_parameterFieldArray[0]].ToString(),
                                _oReaderParams[_parameterFieldArray[1]].ToString());
                    }
                }
                _rptDocument.DataDefinition.ParameterFields[_parameterName].ApplyDefaultValues(_defaultValues);
            }
            catch (Exception ex)
            {
                SendEmailLog("Error reading SQL table " + _parameterTable + ": " + ex.Message);
                if (_rptDocument != null)
                    _rptDocument.Dispose();
                if (_oReaderRefresh != null)
                    _oReaderRefresh.Close();
                if (_oCommandRefresh != null)
                    _oCommandRefresh.Dispose();
                if (_conMas != null)
                    _conMas.Close();
            }
            finally
            {
                if (_oReaderParams != null)
                    _oReaderParams.Close();
                if (_oCommandParams != null)
                    _oCommandParams.Dispose();
                if (_conParams != null)
                    _conParams.Close();
            }
        }

        private static void SaveReport()
        {
            try
            {
                File.Delete(ReportDirectoryTemp + _reportFile);
                _rptDocument.SaveAs(ReportDirectoryTemp + _reportFile);
                File.Copy(ReportDirectoryTemp + _reportFile, ReportDirectory + _reportFile, true);
            }
            catch (Exception ex)
            {
                SendEmailLog("Error saving Crystal Report " + _reportFile + ": " + ex.Message);
                if (_oReaderRefresh != null)
                    _oReaderRefresh.Close();
                if (_oCommandRefresh != null)
                    _oCommandRefresh.Dispose();
                if (_conMas != null)
                    _conMas.Close();
            }
            finally
            {
                if (_rptDocument != null)
                    _rptDocument.Dispose();
            }
        }

        private static void LoadReportData()
        {

            _parameterName = _oReaderRefresh["ParameterName"].ToString();
            _parameterFieldArray = (_oReaderRefresh["ParameterField"].ToString()).Split(',');
            _parameterField = (_parameterFieldArray.Length == 1)
                ? "[" + _parameterFieldArray[0] + "]"
                : "[" + _parameterFieldArray[0] + "],[" + _parameterFieldArray[1] + "]";
            _parameterFieldName = _oReaderRefresh["ParameterField"].ToString();
            _parameterTable = "[" + _oReaderRefresh["ParameterTable"] + "]";
            _parameterAll = (bool) _oReaderRefresh["ParameterAll"];
            _reportFile = _oReaderRefresh["ReportID"].ToString();
        }


        static void Main()
        {
      
            try
            {
                _conMas = new SqlConnection(SsqlConnection);
                _conMas.Open();
                _oCommandRefresh =
                    new SqlCommand(
                        "SELECT [ReportName], [ParameterName], [ParameterField], [ParameterTable], [ParameterAll], [ReportID] FROM [CrystalParametersView]",
                        _conMas);
                _oReaderRefresh = _oCommandRefresh.ExecuteReader();
                while (_oReaderRefresh.Read())
                {
                    LoadReportData();
                    if (IsReportMissing) continue;
                    LoadReport();
                    LoadParameters();
                    SaveReport();
                }
                
            }
            catch (Exception ex)
            {
                SendEmailLog("Error reading SQL table CrystalParameters " + ex.Message);
                if (_rptDocument!=null)
                    _rptDocument.Dispose();
                if (_oReaderParams!=null)
                    _oReaderParams.Close();
                if (_oCommandParams != null)
                    _oCommandParams.Dispose();
                if (_conParams != null)
                    _conParams.Close();
                GC.Collect();
                return;
            }
            finally
            {
                if (_oReaderRefresh!=null)
                    _oReaderRefresh.Close();
                if (_oCommandRefresh!=null)
                    _oCommandRefresh.Dispose();
                if (_conMas!=null)
                    _conMas.Close();
            }
            SendEmailLog("Daily Refresh completed!");
       }
    }
}
