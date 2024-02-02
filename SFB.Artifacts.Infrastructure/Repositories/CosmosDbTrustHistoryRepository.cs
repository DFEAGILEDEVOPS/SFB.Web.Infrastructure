using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using SFB.Web.ApplicationCore;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.Infrastructure.Logging;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using SFB.Artifacts.Infrastructure.Helpers;


namespace SFB.Web.Infrastructure.Repositories
{
    public class CosmosDbTrustHistoryRepository : AppInsightsLoggable, ITrustHistoryRepository
    {
        private readonly string _databaseId;
        private readonly string _collectionId;
        private static CosmosClient _client;

        public CosmosDbTrustHistoryRepository(ILogManager logManager) : base(logManager)
        {
            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = ConfigurationManager.AppSettings[AppSettings.DisableCosmosConnectionModeDirect] == bool.TrueString
                ? clientBuilder.Build() 
                : clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = _databaseId = ConfigurationManager.AppSettings["database"];

            _collectionId = ConfigurationManager.AppSettings["trustHistoryCollection"];
        }

        public async Task<TrustHistoryDataObject> GetTrustHistoryDataObjectAsync(int uid)
        {
            var container = _client.GetContainer(_databaseId, _collectionId);

            var queryString = $"SELECT * FROM c WHERE c.UID=@UID";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@UID", uid);

            var feedIterator = container.GetItemQueryIterator<TrustHistoryDataObject>(queryDefinition, null);
            var result = (await feedIterator.ReadNextAsync()).FirstOrDefault();

            return result;
        }
    }
}
