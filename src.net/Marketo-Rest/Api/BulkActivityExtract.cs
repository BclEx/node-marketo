using Marketo.Services;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading.Tasks;

namespace Marketo.Api
{
    /// <summary>
    /// Class BulkActivityExtract.
    /// </summary>
    public class BulkActivityExtract
    {
        readonly Action<string> _log = util.logger;
        readonly MarketoClient _marketo;
        readonly Connection _connection;
        readonly Retry _retry;

        /// <summary>
        /// Initializes a new instance of the <see cref="BulkActivityExtract"/> class.
        /// </summary>
        /// <param name="marketo">The marketo.</param>
        /// <param name="connection">The connection.</param>
        public BulkActivityExtract(MarketoClient marketo, Connection connection)
        {
            _marketo = marketo;
            _connection = connection;
            _retry = new Retry(new { maxRetries = 10, initialDelay = 30000, maxDelay = 60000 });
        }

        /// <summary>
        /// Creates the specified filter.
        /// </summary>
        /// <param name="filter">The filter.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public Task<dynamic> Create(dynamic filter, dynamic options = null)
        {
            var path = util.createBulkPath("activities", "export", "create.json");
            options = dyn.exp(options);
            options.filter = filter;
            return _connection.postJson(path, options);
        }

        /// <summary>
        /// Enqueues the specified export identifier.
        /// </summary>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public Task<dynamic> Enqueue(string exportId, dynamic options = null)
        {
            var path = util.createBulkPath("activities", "export", exportId, "enqueue.json");
            return _connection.post(path, new { data = options });
        }

        /// <summary>
        /// Statuses the specified export identifier.
        /// </summary>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public Task<dynamic> Status(string exportId, dynamic options = null)
        {
            var path = util.createBulkPath("activities", "export", exportId, "status.json");
            return _connection.get(path, new { data = options });
        }

        /// <summary>
        /// Statuses the til completed.
        /// </summary>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public async Task<dynamic> StatusTilCompleted(string exportId, dynamic options = null)
        {
            Func<bool, Task<object>> requestFn = async (forceOAuth) =>
            {
                var data = await Status(exportId);
                if (!(bool)data.success)
                {
                    var msg = (string)data.errors[0].message;
                    _log(msg);
                    throw new MarketoException(msg);
                }
                Console.WriteLine($"STATUS: {data.result[0].status}");
                if (data.result[0].status == "Queued" || data.result[0].status == "Processing")
                    throw new MarketoException(null)
                    {
                        Id = data["requestId"],
                        Errors = new JArray(new JObject(new JProperty("code", "606"))),
                        Code = 606
                    };
                return data;
            };
            return await _retry.start(requestFn);
        }

        /// <summary>
        /// Cancels the specified export identifier.
        /// </summary>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public Task<dynamic> Cancel(string exportId, dynamic options = null)
        {
            var path = util.createBulkPath("activities", "export", exportId, "cancel.json");
            return _connection.post(path, new { data = options });
        }

        /// <summary>
        /// Gets the specified filter.
        /// </summary>
        /// <param name="filter">The filter.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        /// <exception cref="MarketoException">
        /// </exception>
        public async Task<string> QueueAndWaitTilComplete(dynamic filter, dynamic options = null)
        {
            var data = await Create(filter, options);
            if (!(bool)data.success)
            {
                var msg = (string)data.errors[0].message;
                _log(msg);
                throw new MarketoException(msg);
            }
            var exportId = (string)data.results[0].exportId;
            try
            {
                data = await Enqueue(exportId);
                if (!(bool)data.success)
                {
                    var msg = (string)data.errors[0].message;
                    _log(msg);
                    throw new MarketoException(msg);
                }
                data = await StatusTilCompleted(exportId);
                if (!(bool)data.success)
                {
                    var msg = (string)data.errors[0].message;
                    _log(msg);
                    throw new MarketoException(msg);
                }
            }
            catch (Exception err) { await Cancel(exportId); throw err; }
            return exportId;
        }

        /// <summary>
        /// Files the specified export identifier.
        /// </summary>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>Task&lt;dynamic&gt;.</returns>
        public Task<dynamic> File(string exportId, dynamic options = null)
        {
            var path = util.createBulkPath("activities", "export", exportId, "file.json");
            return _connection.get(path, new { data = options });
        }

        /// <summary>
        /// trans file as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action">The action.</param>
        /// <param name="exportId">The export identifier.</param>
        /// <param name="options">The options.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public async Task<IEnumerable<T>> TransFileAsync<T>(Func<Collection<string>, T> action, string exportId, dynamic options = null)
        {
            var file = await File(exportId, options);
            using (var sr = new StringReader(file))
            {
                var cr = new CsvReader();
                return cr.Execute(sr, action);
            }
        }
    }
}
