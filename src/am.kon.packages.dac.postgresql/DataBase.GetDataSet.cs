using System.Data;
using am.kon.packages.dac.postgresql.Extensions;
using am.kon.packages.dac.primitives;
using Npgsql;

namespace am.kon.packages.dac.postgresql;

public partial class DataBase : IDataBase
{
    public DataSet GetDataSet(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }

    public DataSet GetDataSet(string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters, commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }

    public DataSet GetDataSet(string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }
    
    public DataSet GetDataSet(string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }
    
    public DataSet GetDataSet(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }
    
    public DataSet GetDataSet(string sql, dynamic parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }

    public DataSet GetDataSet<T>(string sql, T parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        DataSet ds = new DataSet();

        FillData<DataSet>(ds, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);

        return ds;
    }
}