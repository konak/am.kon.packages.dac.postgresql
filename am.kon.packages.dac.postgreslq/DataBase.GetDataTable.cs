using System.Data;
using am.kon.packages.dac.postgreslq.Extensions;
using am.kon.packages.dac.primitives;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    public DataTable GetDataTable(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable(string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable(string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable(string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable(string sql, dynamic parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }

    public DataTable GetDataTable<T>(string sql, T parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataTable dt = new DataTable("Table0");

        FillData<DataTable>(dt, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return dt;
    }
}