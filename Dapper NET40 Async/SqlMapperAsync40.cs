using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dapper
{
#if !CSHARP30
	public static partial class SqlMapper
	{
		public static Task<int> ExecuteAsync(this IDbConnection cnn, string sql, dynamic param = null, IDbTransaction transaction = null,
											 int? commandTimeout = null, CommandType? commandType = null,
											 CancellationToken token = default(CancellationToken))
		{
			IEnumerable multiExec = (object) param as IEnumerable;
			if (multiExec != null && !(multiExec is string))
				return ExecuteMultiAsync(cnn, sql, multiExec, transaction, commandTimeout, commandType, token);
			// nice and simple
			CacheInfo info = null;
			if ((object) param != null)
			{
				Identity identity = new Identity(sql, commandType, cnn, null, (object) param == null ? null : ((object) param).GetType(), null);
				info = GetCacheInfo(identity);
			}
			return ExecuteCommandAsync(cnn, transaction, sql, (object) param == null ? null : info.ParamReader, (object) param, commandTimeout,
			                           commandType, token);
		}

		private static Task<int> ExecuteCommandAsync(IDbConnection cnn, IDbTransaction transaction, string sql,
		                                             Action<IDbCommand, object> paramReader, object obj, int? commandTimeout,
		                                             CommandType? commandType, CancellationToken token)
		{
			return ExecuteInTask(() =>
			                     {
				                     IDbCommand cmd = null;
				                     CancellationTokenRegistration registration = new CancellationTokenRegistration();
				                     bool wasClosed = cnn.State == ConnectionState.Closed;
				                     try
				                     {
					                     cmd = SetupCommand(cnn, transaction, sql, paramReader, obj, commandTimeout, commandType);
					                     if (token.CanBeCanceled)
						                     registration = token.Register(CancelIgnoreFailure, cmd);
					                     if (wasClosed)
						                     cnn.Open();
					                     return cmd.ExecuteNonQuery();
				                     }
				                     catch
				                     {
					                     token.ThrowIfCancellationRequested();
					                     throw;
				                     }
				                     finally
				                     {
					                     registration.Dispose();
					                     if (wasClosed)
						                     cnn.Close();
					                     if (cmd != null)
						                     cmd.Dispose();
				                     }
			                     }, token);
		}

		private static Task<int> ExecuteMultiAsync(IDbConnection cnn, string sql, IEnumerable multiExec, IDbTransaction transaction,
		                                           int? commandTimeout, CommandType? commandType, CancellationToken token)
		{
			return ExecuteInTask(() =>
			                     {
				                     CacheInfo info = null;
				                     bool isFirst = true;
				                     int total = 0;
				                     CancellationTokenRegistration registration = new CancellationTokenRegistration();
				                     using (var cmd = SetupCommand(cnn, transaction, sql, null, null, commandTimeout, commandType))
				                     {
					                     string masterSql = null;
					                     if (token.CanBeCanceled)
						                     registration = token.Register(CancelIgnoreFailure, cmd);
					                     using (registration)
					                     {
						                     foreach (var obj in multiExec)
						                     {
							                     token.ThrowIfCancellationRequested();
							                     if (isFirst)
							                     {
								                     masterSql = cmd.CommandText;
								                     isFirst = false;
								                     Identity identity = new Identity(sql, cmd.CommandType, cnn, null, obj.GetType(), null);
								                     info = GetCacheInfo(identity);
							                     }
							                     else
							                     {
								                     cmd.CommandText = masterSql; // because we do magic replaces on "in" etc
								                     cmd.Parameters.Clear(); // current code is Add-tastic
							                     }
							                     info.ParamReader(cmd, obj);
							                     total += cmd.ExecuteNonQuery();
						                     }
					                     }
				                     }
				                     return total;
			                     }, token);

		}

		public static Task<IEnumerable<T>> QueryAsync<T>(this IDbConnection cnn, string sql, dynamic param = null,
		                                                 IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null,
		                                                 CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			//var dataAsync = QueryInternalAsync<T>(cnn, sql, param as object, transaction, commandTimeout, commandType, token);
			var dataAsync = QueryInternalAsync2<T>(cnn, sql, param as object, transaction, commandTimeout, commandType, token);
			if (buffered)
				return dataAsync.ContinueWith(data => (IEnumerable<T>)data.Result.ToList(), token,
				                              TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously,
				                              TaskScheduler.Default);
			return dataAsync;
		}

		public static Task<IEnumerable<dynamic>> QueryAsync(this IDbConnection cnn, string sql, dynamic param = null,
		                                                    IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null,
		                                                    CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			//var dataAsync = QueryInternalAsync<DapperRow>(cnn, sql, param as object, transaction, commandTimeout, commandType, token);
			var dataAsync = QueryInternalAsync2<DapperRow>(cnn, sql, param as object, transaction, commandTimeout, commandType, token);
			if (buffered)
				return dataAsync.ContinueWith(data => (IEnumerable<dynamic>)data.Result.Cast<dynamic>().ToList(), token,
				                              TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously,
				                              TaskScheduler.Default);
			return dataAsync.ContinueWith(t => t.Result.Cast<dynamic>(), token,
											  TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously,
											  TaskScheduler.Default);
		}

		private static Task<IEnumerable<T>> QueryInternalAsync<T>(IDbConnection cnn, string sql, object param, IDbTransaction transaction, int? commandTimeout, CommandType? commandType, CancellationToken token)
		{
			return ExecuteInTask(() =>
			{ // NOTE: this version reads all output to buffer
				var identity = new Identity(sql, commandType, cnn, typeof(T), param == null ? null : param.GetType(), null);
				var info = GetCacheInfo(identity);

				IDbCommand cmd = null;
				IDataReader reader = null;

				CancellationTokenRegistration registration = new CancellationTokenRegistration();
				bool wasClosed = cnn.State == ConnectionState.Closed;
				try
				{
					cmd = SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);
					if (token.CanBeCanceled)
						registration = token.Register(CancelIgnoreFailure, cmd);

					if (wasClosed) cnn.Open();
					reader = cmd.ExecuteReader(wasClosed ? CommandBehavior.CloseConnection : CommandBehavior.Default);
					wasClosed = false; // *if* the connection was closed and we got this far, then we now have a reader
					// with the CloseConnection flag, so the reader will deal with the connection; we
					// still need something in the "finally" to ensure that broken SQL still results
					// in the connection closing itself
					var tuple = info.Deserializer;
					int hash = GetColumnHash(reader);
					if (tuple.Func == null || tuple.Hash != hash)
					{
						tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(typeof(T), reader, 0, -1, false));
						SetQueryCache(identity, info);
					}

					var func = tuple.Func;

					var result = new List<T>();
					while (reader.Read())
					{
						result.Add((T)func(reader));
					}
					// happy path; close the reader cleanly - no
					// need for "Cancel" etc
					reader.Dispose();
					reader = null;

					return (IEnumerable<T>)result;
				}
				catch
				{
					token.ThrowIfCancellationRequested();
					throw;
				}
				finally
				{
					registration.Dispose();
					if (reader != null)
					{
						if (!reader.IsClosed) try { cmd.Cancel(); }
							catch { /* don't spoil the existing exception */ }
						reader.Dispose();
					}
					if (wasClosed) cnn.Close();
					if (cmd != null) cmd.Dispose();
				}
			}, token);
		}

		private static Task<IEnumerable<T>> QueryInternalAsync2<T>(IDbConnection cnn, string sql, object param, IDbTransaction transaction, int? commandTimeout, CommandType? commandType, CancellationToken token)
		{
			return ExecuteInTask(() =>
			{ // NOTE: this version mimics original dapper buffer behaviour
				var identity = new Identity(sql, commandType, cnn, typeof (T), param == null ? null : param.GetType(), null);
				var info = GetCacheInfo(identity);

				IDbCommand cmd = null;

				CancellationTokenRegistration registration = new CancellationTokenRegistration();
				bool wasClosed = cnn.State == ConnectionState.Closed;
				try
				{
					cmd = SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);
					if (token.CanBeCanceled)
						registration = token.Register(CancelIgnoreFailure, cmd);

					if (wasClosed)
						cnn.Open();
					return Tuple.Create(cmd.ExecuteReader(wasClosed ? CommandBehavior.CloseConnection : CommandBehavior.Default),
					                    identity,
					                    info,
					                    cmd);
				}
				catch
				{
					token.ThrowIfCancellationRequested();
					throw;
				}
				finally
				{
					registration.Dispose();
					if (wasClosed)
						cnn.Close();
					if (cmd != null)
						cmd.Dispose();
				}
			}, token)
			.ContinueWith(t => ReadData<T>(token, t), token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
		}

		private static IEnumerable<T> ReadData<T>(CancellationToken token, Task<Tuple<IDataReader, Identity, CacheInfo, IDbCommand>> t)
		{
			IDataReader reader = null;
			IDbCommand cmd = null;
			try
			{
				reader = t.Result.Item1;
				var identity = t.Result.Item2;
				var info = t.Result.Item3;
				cmd = t.Result.Item4;

				var tuple = info.Deserializer;
				int hash = GetColumnHash(reader);
				if (tuple.Func == null || tuple.Hash != hash)
				{
					tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(typeof (T), reader, 0, -1, false));
					SetQueryCache(identity, info);
				}

				var func = tuple.Func;

				while (reader.Read())
				{
					token.ThrowIfCancellationRequested();
					yield return (T)func(reader);
				}
				// happy path; close the reader cleanly - no
				// need for "Cancel" etc
				reader.Dispose();
				reader = null;
			}
			finally
			{
				if (reader != null)
				{
					if (!reader.IsClosed)
						CancelIgnoreFailure(cmd);
					reader.Dispose();
				}
			}
		}

		#region MultiQuery

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst">The first type in the recordset</typeparam>
		/// <typeparam name="TSecond">The second type in the recordset</typeparam>
		/// <typeparam name="TReturn">The return type</typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn">The Field we should split and read the second object from (default: id)</param>
		/// <param name="commandTimeout">Number of seconds before command execution timeout</param>
		/// <param name="commandType">Is it a stored proc or a batch?</param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, DontMap, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn">The Field we should split and read the second object from (default: id)</param>
		/// <param name="commandTimeout">Number of seconds before command execution timeout</param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, TThird, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		/// <summary>
		/// Perform a multi mapping query with 4 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn"></param>
		/// <param name="commandTimeout"></param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		/// <summary>
		/// Perform a multi mapping query with 5 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TFifth"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn"></param>
		/// <param name="commandTimeout"></param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		/// <summary>
		/// Perform a multi mapping query with 6 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TFifth"></typeparam>
		/// <typeparam name="TSixth"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn"></param>
		/// <param name="commandTimeout"></param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, DontMap, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		/// <summary>
		/// Perform a multi mapping query with 7 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TFifth"></typeparam>
		/// <typeparam name="TSixth"></typeparam>
		/// <typeparam name="TSeventh"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn"></param>
		/// <param name="commandTimeout"></param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this IDbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, dynamic param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null, CancellationToken token = default(CancellationToken))
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(cnn, sql, map, param as object, transaction, buffered, splitOn, commandTimeout, commandType, token);
		}

		#endregion

		private static Task<IEnumerable<TReturn>> MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this IDbConnection cnn, string sql, object map, object param, IDbTransaction transaction, bool buffered, string splitOn, int? commandTimeout, CommandType? commandType, CancellationToken token)
		{
			throw new NotImplementedException();
			//var identity = new Identity(sql, commandType, cnn, typeof(TFirst), (object)param == null ? null : ((object)param).GetType(), new[] { typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth), typeof(TFifth), typeof(TSixth), typeof(TSeventh) });
			//var info = GetCacheInfo(identity);
			//var cmd = (DbCommand)SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);
			//using (var reader = cmd.ExecuteReaderAsync())
			//{
			//    var results = MultiMapImpl<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(null, null, map, null, null, splitOn, null, null, reader, identity);
			//    return buffered ? results.ToList() : results;
			//}
		}

		internal static Task<TResult> ExecuteInTask<TResult>(Func<TResult> asyncTask, CancellationToken token)
		{
			TaskCompletionSource<TResult> source = new TaskCompletionSource<TResult>();
			try
			{
				if (token.CanBeCanceled && token.IsCancellationRequested)
				{
					source.SetCanceled();
					return source.Task;
				}
				return Task<TResult>.Factory.StartNew(asyncTask, token);
			}
			catch (Exception ex)
			{
				source.SetException(ex);
				return source.Task;
			}
		}

		private static void CancelIgnoreFailure(object state)
		{
			IDbCommand cmd = state as IDbCommand;
			if (cmd != null)
			{
				try
				{
					cmd.Cancel();
				}
				catch
				{
					/* don't spoil the existing exception */
				}
			}
		}
	}
#endif
}