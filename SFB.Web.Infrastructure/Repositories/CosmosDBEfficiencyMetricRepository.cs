using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.Infrastructure.Logging;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Repositories
{
    public class CosmosDBEfficiencyMetricRepository : AppInsightsLoggable, IEfficiencyMetricRepository
    {
        private readonly string _databaseId;
        private static CosmosClient _client;

        public CosmosDBEfficiencyMetricRepository(ILogManager logManager) : base(logManager)
        {
            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = _databaseId = ConfigurationManager.AppSettings["database"];
        }

        public CosmosDBEfficiencyMetricRepository(CosmosClient cosmosClient, string databaseId, ILogManager logManager) : base(logManager)
        {
            _client = cosmosClient;
            _databaseId = databaseId;
        }

        public async Task<EfficiencyMetricParentDataObject> GetEfficiencyMetricDataObjectByUrnAsync(int urn)
        {
            var container = _client.GetContainer(_databaseId, "EmData");

            var queryString = $"SELECT * FROM c WHERE c.Urn=@URN";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@URN", urn);

            var feedIterator = container.GetItemQueryIterator<EfficiencyMetricParentDataObject>(queryDefinition, null);
            return (await feedIterator.ReadNextAsync()).First();            
        }
    }
}
