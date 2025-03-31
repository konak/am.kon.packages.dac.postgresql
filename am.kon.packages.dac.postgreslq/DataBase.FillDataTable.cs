using System.Data;
using am.kon.packages.dac.postgreslq.Extensions;
using am.kon.packages.dac.primitives;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    public void FillDataTable(DataTable dt, string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dt, string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dt, string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dt, string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dt, string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable(DataTable dt, string sql, dynamic parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillDataTable<T>(DataTable dt, string sql, T parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }
}