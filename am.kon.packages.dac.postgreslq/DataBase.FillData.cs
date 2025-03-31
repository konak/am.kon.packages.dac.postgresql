using System.Data;
using am.kon.packages.dac.postgreslq.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Constants.Exception;
using am.kon.packages.dac.primitives.Exceptions;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    public void FillData<T>(T dataOut, string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        NpgsqlCommand cmd = null;
        NpgsqlDataAdapter da = null;

        try
        {
            cmd = new NpgsqlCommand(sql, new NpgsqlConnection(this._connectionString));
            cmd.CommandType = commandType;

            NpgsqlParameter rv = new NpgsqlParameter("@return_value", SqlDbType.Int);
            rv.Direction = ParameterDirection.ReturnValue;

            cmd.Parameters.Add(rv);

            if (parameters != null && parameters.Length > 0)
                cmd.Parameters.AddRange(parameters);

            da = new NpgsqlDataAdapter(cmd);

            switch (dataOut)
            {
                case DataTable:
                    if (maxRecords == 0)
                        da.Fill(dataOut as DataTable);
                    else
                        da.Fill(startRecord, maxRecords, new DataTable[] { dataOut as DataTable });

                    break;

                case DataSet:
                    if (maxRecords == 0)
                        da.Fill(dataOut as DataSet);
                    else
                        da.Fill(dataOut as DataSet, startRecord, maxRecords, string.Empty);

                    break;

                default:
                    if (throwSystemException)
                        throw new DacGenericException(Messages.FILL_DATA_INVALID_TYPE_PASSED + typeof(T).ToString());
                    break;
            }

            int retVal = (int)rv.Value;

            if (retVal != 0)
                throw new DacSqlExecutionReturnedErrorCodeException(retVal, dataOut);
        }
        catch (NpgsqlException ex)
        {
            if (throwDBException)
                throw new DacSqlExecutionException(ex);
        }
        catch (DacSqlExecutionReturnedErrorCodeException)
        {
            throw;
        }
        catch (DacGenericException)
        {
            if (throwGenericException)
                throw;
        }
        catch (Exception ex)
        {
            if (throwSystemException)
                throw new DacGenericException(Messages.SYSTEM_EXCEPTION_ON_EXECUTE_SQL_BATCH_LEVEL, ex);
        }
        finally
        {
            try
            {
                if (cmd != null)
                    cmd.Connection?.Close();
            }
            catch (Exception ex)
            {
                if (throwSystemException)
                    throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
            }
        }
    }

    public void FillData<T>(T dataOut, string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillData<T>(T dataOut, string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillData<T>(T dataOut, string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillData<T>(T dataOut, string sql, dynamic parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0,
        int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

    public void FillData<T>(T dataOut, string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true,
        int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }
    
    public void FillData<T, TParam>(T dataOut, string sql, TParam parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true, int startRecord = 0, int maxRecords = 0)
    {
        FillData(dataOut, sql, parameters.ToDataParameters(), commandType, throwDBException, throwGenericException, throwSystemException, startRecord, maxRecords);
    }

}