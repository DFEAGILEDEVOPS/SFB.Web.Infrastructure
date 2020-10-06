using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.Infrastructure.Logging;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Repositories
{
    public class CosmosDBEfficiencyMetricRepository : AppInsightsLoggable, IEfficiencyMetricRepository
    {
        private readonly string _databaseId;
        private readonly string _collectionId;
        private static CosmosClient _client;

        public CosmosDBEfficiencyMetricRepository(ILogManager logManager) : base(logManager)
        {
            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = ConfigurationManager.AppSettings["database"];

            _collectionId = ConfigurationManager.AppSettings["emCollection"];
        }

        public CosmosDBEfficiencyMetricRepository(CosmosClient cosmosClient, string databaseId, string collectionId, ILogManager logManager) : base(logManager)
        {
            _client = cosmosClient;
            _databaseId = databaseId;
            _collectionId = collectionId;
        }

        public async Task<List<EfficiencyMetricParentDataObject>> GetEfficiencyMetricDataObjectByUrnAsync(int urn)
        {
            var container = _client.GetContainer(_databaseId, _collectionId);

            var queryString = $"SELECT * FROM c WHERE c.Urn=@URN";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@URN", urn);

            var feedIterator = container.GetItemQueryIterator<EfficiencyMetricParentDataObject>(queryDefinition, null);
            List<EfficiencyMetricParentDataObject> results = new List<EfficiencyMetricParentDataObject>();
            while (feedIterator.HasMoreResults)
            {
                var response = await feedIterator.ReadNextAsync();

                results.AddRange(response.ToList());
            }

            return results;            
        }

        public async Task<bool> GetStatusByUrnAsync(int urn)
        {
            var container = _client.GetContainer(_databaseId, _collectionId);

            var queryString = $"SELECT c.Urn FROM c WHERE c.Urn=@URN";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@URN", urn);

            var feedIterator = container.GetItemQueryIterator<object>(queryDefinition, null);
            return (await feedIterator.ReadNextAsync()).FirstOrDefault() != null;
        }
    }
}
