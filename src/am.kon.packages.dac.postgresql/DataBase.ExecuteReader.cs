using System.Data;
using am.kon.packages.dac.postgresql.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Exceptions;
using Npgsql;

namespace am.kon.packages.dac.postgresql;

public partial class DataBase : IDataBase
{
    internal async Task<IDataReader> ExecuteReaderAsyncInternal(IDbConnection connection, string sqlQuery, IDataParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        NpgsqlConnection conn = connection as NpgsqlConnection;
        NpgsqlCommand sqlCommand = new NpgsqlCommand(sqlQuery, conn);

        sqlCommand.CommandType = commandType;

        NpgsqlParameter returnValue = new NpgsqlParameter("@return_value", SqlDbType.Int);
        returnValue.Direction = ParameterDirection.ReturnValue;
        returnValue.IsNullable = false;

        sqlCommand.Parameters.Add(returnValue);

        if (parameters != null && parameters.Length > 0)
            sqlCommand.Parameters.AddRange(parameters);

        NpgsqlDataReader res = await sqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection, _cancellationToken);

        int retVal = 0;

        if (returnValue.Value != null)
            retVal = (int)returnValue.Value;

        if (retVal != 0)
            throw new DacSqlExecutionReturnedErrorCodeException(retVal, res);

        return res;
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters, commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters, commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters.ToDataParameters(), commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters.ToDataParameters(), commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }
    
    public Task<IDataReader> ExecuteReaderAsync(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters.ToDataParameters(), commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync(string sql, dynamic parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters.ToDataParameters(), commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }

    public Task<IDataReader> ExecuteReaderAsync<T>(string sql, T parameters, CommandType commandType = CommandType.Text, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        Func<IDbConnection, Task<IDataReader>> executeReaderAsyncFunction = connection => ExecuteReaderAsyncInternal(connection, sql, parameters.ToDataParameters(), commandType);

        return ExecuteSQLBatchAsync<IDataReader>(executeReaderAsyncFunction, false, throwDBException, throwGenericException, throwSystemException);
    }
}