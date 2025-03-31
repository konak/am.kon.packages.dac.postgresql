using System.Data;
using am.kon.packages.dac.postgreslq.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Exceptions;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    public Task<object> ExecuteScalarAsync(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        Func<IDbConnection, Task<object>> b = async delegate (IDbConnection connection)
        {
            NpgsqlConnection conn = connection as NpgsqlConnection;
            NpgsqlCommand cmd = new NpgsqlCommand(sql, conn);

            cmd.CommandType = commandType;

            NpgsqlParameter rv = new NpgsqlParameter("@return_value", SqlDbType.Int);
            rv.Direction = ParameterDirection.ReturnValue;

            cmd.Parameters.Add(rv);

            if (parameters != null && parameters.Length > 0)
                cmd.Parameters.AddRange(parameters);

            object res = await cmd.ExecuteScalarAsync(_cancellationToken);

            int retVal = (int)rv.Value;

            if (retVal != 0)
                throw new DacSqlExecutionReturnedErrorCodeException(retVal, res);

            return res;
        };

        return ExecuteSQLBatchAsync<object>(b);
    }

    public Task<object> ExecuteScalarAsync(string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync(string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync(string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync(string sql, dynamic parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<object> ExecuteScalarAsync<T>(string sql, T parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteScalarAsync(sql, parameters.ToDataParameters(), commandType);
    }
}