using System.Data;
using am.kon.packages.dac.primitives;
using am.kon.packages.dac.primitives.Constants.Exception;
using am.kon.packages.dac.primitives.Exceptions;
using Npgsql;

namespace am.kon.packages.dac.postgreslq;

public partial class DataBase : IDataBase
{
    private readonly Type _dataTableType = typeof(DataTable);
    private readonly Type _dataSetType = typeof(DataSet);
    private readonly string _connectionString;
    private readonly CancellationToken _cancellationToken;

    /// <summary>
    /// Gets the connection string used to connect to the PostgreSQL database.
    /// </summary>
    /// <remarks>
    /// The connection string contains the necessary details for establishing a connection to the PostgreSQL database, such as server address, database name, user credentials, and any other connection parameters.
    /// </remarks>
    public string ConnectionString
    {
        get { return _connectionString; }
    }


    /// <summary>
    /// Initializes a new instance of the <see cref="DataBase"/> class with the specified connection string and cancellation token.
    /// </summary>
    /// <param name="connectionString">The connection string used to connect to the PostgreSQL database.</param>
    /// <param name="cancellationToken">A token to signal cancellation of asynchronous operations.</param>
    public DataBase(string connectionString, CancellationToken cancellationToken)
    {
        _connectionString = connectionString;
        _cancellationToken = cancellationToken;
    }

    /// <summary>
    /// Executes an asynchronous SQL batch operation.
    /// </summary>
    /// <typeparam name="T">The type of the result returned by the batch operation.</typeparam>
    /// <param name="batch">A function representing the batch operation to execute, which accepts an <see cref="IDbConnection"/> and returns a <see cref="Task{T}"/>.</param>
    /// <param name="closeConnection">Indicates whether to close the database connection after the operation completes. Default is true.</param>
    /// <param name="throwDBException">Indicates whether to throw a <see cref="DacSqlExecutionException"/> in case of SQL-related issues. Default is true.</param>
    /// <param name="throwGenericException">Indicates whether to throw a <see cref="DacGenericException"/> in case of generic errors. Default is true.</param>
    /// <param name="throwSystemException">Indicates whether to throw a <see cref="DacGenericException"/> for unexpected system exceptions. Default is true.</param>
    /// <returns>A <see cref="Task{T}"/> representing the result of the batch operation.</returns>
    /// <exception cref="DacSqlExecutionException">Thrown if an SQL-related exception occurs and <paramref name="throwDBException"/> is true.</exception>
    /// <exception cref="DacGenericException">Thrown if a system exception occurs during execution and <paramref name="throwGenericException"/> or <paramref name="throwSystemException"/> is true.</exception>
    public async Task<T> ExecuteSQLBatchAsync<T>(Func<IDbConnection, Task<T>> batch, bool closeConnection = true, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        T res = default;
        NpgsqlConnection connection = null;

        try
        {
            connection = new NpgsqlConnection(this._connectionString);
            await connection.OpenAsync(_cancellationToken);
            res = await batch(connection);
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
                if (closeConnection && connection != null)
                    await connection.CloseAsync();
            }
            catch (Exception ex)
            {
                if (throwSystemException)
                    throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
            }
        }

        return res;
    }

    /// <summary>
    /// Executes an asynchronous transactional SQL batch operation.
    /// </summary>
    /// <typeparam name="T">The type of the result returned by the batch operation.</typeparam>
    /// <param name="batch">A function representing the batch operation to execute, which accepts an <see cref="IDbTransaction"/> and returns a <see cref="Task{T}"/>.</param>
    /// <param name="closeConnection">Indicates whether to close the database connection after the operation completes. Default is true.</param>
    /// <param name="throwDBException">Indicates whether to throw a <see cref="DacSqlExecutionException"/> in case of SQL-related issues. Default is true.</param>
    /// <param name="throwGenericException">Indicates whether to throw a <see cref="DacGenericException"/> in case of generic errors. Default is true.</param>
    /// <param name="throwSystemException">Indicates whether to throw a <see cref="DacGenericException"/> for unexpected system exceptions. Default is true.</param>
    /// <returns>A <see cref="Task{T}"/> representing the result of the batch operation.</returns>
    /// <exception cref="DacSqlExecutionException">Thrown if an SQL-related exception occurs and <paramref name="throwDBException"/> is true.</exception>
    /// <exception cref="DacGenericException">Thrown if a system exception occurs during execution and <paramref name="throwGenericException"/> or <paramref name="throwSystemException"/> is true.</exception>
    public async Task<T> ExecuteTransactionalSQLBatchAsync<T>(Func<IDbTransaction, Task<T>> batch, bool closeConnection = true, bool throwDBException = true, bool throwGenericException = true, bool throwSystemException = true)
    {
        T res = default;
        NpgsqlConnection connection = null;
        NpgsqlTransaction transaction = null;

        try
        {
            connection = new NpgsqlConnection(this._connectionString);

            await connection.OpenAsync(_cancellationToken);
            transaction = await connection.BeginTransactionAsync(_cancellationToken);
            res = await batch(transaction);
            await transaction.CommitAsync(_cancellationToken);
        }
        catch (NpgsqlException ex)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken);

            if (throwDBException)
                throw new DacSqlExecutionException(ex);
        }
        catch (DacSqlExecutionReturnedErrorCodeException)
        {
            throw;
        }
        catch (DacGenericException)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken);

            if (throwGenericException)
                throw;
        }
        catch (Exception ex)
        {
            if (transaction != null)
                await transaction.RollbackAsync(_cancellationToken);

            if (throwSystemException)
                throw new DacGenericException(Messages.SYSTEM_EXCEPTION_ON_EXECUTE_SQL_BATCH_LEVEL, ex);
        }
        finally
        {
            try
            {
                if (closeConnection && connection != null)
                    await connection.CloseAsync();
            }
            catch (Exception ex)
            {
                if (throwSystemException)
                    throw new DacGenericException(Messages.SQL_CONNECTION_CLOSE_EXCEPTION, ex);
            }
        }

        return res;
    }
}