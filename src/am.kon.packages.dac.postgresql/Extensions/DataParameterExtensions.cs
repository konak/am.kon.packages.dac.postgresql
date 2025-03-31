using System.Data;
using System.Reflection;
using am.kon.packages.dac.common.Cache;
using am.kon.packages.dac.primitives;
using Npgsql;

namespace am.kon.packages.dac.postgresql.Extensions;

/// <summary>
/// Provides extension methods for converting various data structures into arrays of <see cref="IDataParameter"/> objects.
/// These methods facilitate the creation and handling of database parameters in a uniform and streamlined manner.
/// </summary>
public static class DataParameterExtensions
{
    /// <summary>
    /// Converts a collection of <see cref="KeyValuePair{TKey, TValue}"/> objects into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="parameters">The collection of key-value pairs representing parameter names and values to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects corresponding to the input collection.</returns>
    private static IDataParameter[] ToSqlParameters(IEnumerable<KeyValuePair<string, object>> parameters)
    {
        if (parameters == null)
            return Array.Empty<IDataParameter>();

        return parameters.Select(p => new NpgsqlParameter(p.Key, p.Value ?? DBNull.Value))
            .Cast<IDataParameter>()
            .ToArray();
    }

    /// <summary>
    /// Converts an array of <see cref="PropertyInfo"/> objects and a corresponding object containing parameter values into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="properties">An array of <see cref="PropertyInfo"/> objects representing the properties to be converted into parameters.</param>
    /// <param name="parameters">An object containing values for the properties to be converted into <see cref="IDataParameter"/> objects.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects corresponding to the provided properties and parameter values.</returns>
    private static IDataParameter[] PropertyInfoToSqlParameters(PropertyInfo[] properties, object parameters)
    {
        if (properties == null || properties.Length == 0 || parameters == null)
            return Array.Empty<IDataParameter>();

        return properties.Select(p => new NpgsqlParameter(p.Name, p.GetValue(parameters) ?? DBNull.Value))
            .Cast<IDataParameter>()
            .ToArray();
    }

    /// <summary>
    /// Converts an array of <see cref="KeyValuePair{TKey, TValue}"/> objects into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="parameters">The array of key-value pairs representing parameter names and values to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects corresponding to the input key-value pairs.</returns>
    public static IDataParameter[] ToDataParameters(this KeyValuePair<string, object>[] parameters)
        => ToSqlParameters(parameters);

    /// <summary>
    /// Converts a collection of <see cref="KeyValuePair{TKey, TValue}"/> objects into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="parameters">An array of key-value pairs representing the parameter names and values to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects corresponding to the input key-value pairs.</returns>
    public static IDataParameter[] ToDataParameters(this List<KeyValuePair<string, object>> parameters)
        => ToSqlParameters(parameters);

    /// <summary>
    /// Converts various forms of input parameters into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="parameters">The input object containing parameter properties to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects representing the input parameters.</returns>
    public static IDataParameter[] ToDataParameters(this DacSqlParameters parameters)
        => ToSqlParameters(parameters?.ToArray());

    /// <summary>
    /// Converts the given dynamic parameters object into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <param name="parameters">The dynamic input object containing parameter properties to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects representing the input parameters.</returns>
    public static IDataParameter[] ToDataParameters(dynamic parameters)
    {
        if (parameters == null)
            return Array.Empty<IDataParameter>();

        Type type = parameters.GetType();
        PropertyInfo[] properties = PropertyInfoCache.GetProperties(type);

        return PropertyInfoToSqlParameters(properties, parameters);
    }

    /// <summary>
    /// Converts the given parameters object of type <typeparamref name="T"/> into an array of <see cref="IDataParameter"/> objects.
    /// </summary>
    /// <typeparam name="T">The type of the parameters object.</typeparam>
    /// <param name="parameters">The input object containing parameter properties to be converted.</param>
    /// <returns>An array of <see cref="IDataParameter"/> objects representing the input parameters.</returns>
    public static IDataParameter[] ToDataParameters<T>(this T parameters)
    {
        if (parameters == null)
            return Array.Empty<IDataParameter>();

        PropertyInfo[] properties = PropertyInfoCache.GetProperties(typeof(T));

        return PropertyInfoToSqlParameters(properties, parameters);
    }
}