using System.Data;
using am.kon.packages.dac.postgreslq.Extensions;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Exceptions;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    public Task<int> ExecuteNonQueryAsync(string sql, IDataParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        Func<IDbConnection, Task<int>> func = async delegate (IDbConnection connection)
        {
            NpgsqlConnection conn = connection as NpgsqlConnection;
            NpgsqlCommand cmd = new NpgsqlCommand(sql, conn);

            cmd.CommandType = commandType;

            NpgsqlParameter rv = new NpgsqlParameter("@return_value", SqlDbType.Int);
            rv.Direction = ParameterDirection.ReturnValue;

            cmd.Parameters.Add(rv);

            if (parameters != null && parameters.Length > 0)
                cmd.Parameters.AddRange(parameters);

            int res = await cmd.ExecuteNonQueryAsync(_cancellationToken);

            int retVal = (int)rv.Value;

            if (retVal != 0)
                throw new DacSqlExecutionReturnedErrorCodeException(retVal, res);

            return res;
        };

        return ExecuteSQLBatchAsync<int>(func);
    }

    
    public Task<int> ExecuteNonQueryAsync(string sql, NpgsqlParameter[] parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, (IDataParameter[])parameters, commandType);
    }

    public Task<int> ExecuteNonQueryAsync(string sql, KeyValuePair<string, object>[] parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<int> ExecuteNonQueryAsync(string sql, List<KeyValuePair<string, object>> parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<int> ExecuteNonQueryAsync(string sql, DacSqlParameters parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<int> ExecuteNonQueryAsync(string sql, dynamic parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, parameters.ToDataParameters(), commandType);
    }

    public Task<int> ExecuteNonQueryAsync<T>(string sql, T parameters, CommandType commandType = CommandType.Text)
    {
        return ExecuteNonQueryAsync(sql, parameters.ToDataParameters(), commandType);
    }
}