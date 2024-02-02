using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.Infrastructure.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using SFB.Artifacts.Infrastructure.Helpers;

namespace SFB.Web.Infrastructure.Repositories
{
    public class CosmosDBSelfAssesmentDashboardRepository : AppInsightsLoggable, ISelfAssesmentDashboardRepository
    {
        private readonly string _databaseId;
        private readonly string _sadCollectionId;
        private readonly string _sizeLookupCollectionId;
        private readonly string _fsmLookupCollectionId;
        private static CosmosClient _client;

        public CosmosDBSelfAssesmentDashboardRepository(ILogManager logManager) : base(logManager)
        {
            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = AppSettings.CosmosConnectionMode.Gateway.Equals(
                ConfigurationManager.AppSettings[AppSettings.CosmosConnectionMode.Key],
                StringComparison.OrdinalIgnoreCase)
                ? clientBuilder.WithConnectionModeGateway().Build()
                : clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = _databaseId = ConfigurationManager.AppSettings["database"];

            _sadCollectionId = ConfigurationManager.AppSettings["sadCollection"];
            
            _sizeLookupCollectionId = ConfigurationManager.AppSettings["sadSizeLookupCollection"];
            
            _fsmLookupCollectionId = ConfigurationManager.AppSettings["sadFSMLookupCollection"];
        }

        public CosmosDBSelfAssesmentDashboardRepository(CosmosClient cosmosClient, 
            string databaseId, 
            string sadCollectionId,
            string sizeCollectionId,
            string fsmCollectionId,
            ILogManager logManager) : base(logManager)
        {
            _client = cosmosClient;
            _databaseId = databaseId;
            _sadCollectionId = sadCollectionId;
            _sizeLookupCollectionId = sizeCollectionId;
            _fsmLookupCollectionId = fsmCollectionId;
        }

        async Task<SADSizeLookupDataObject> ISelfAssesmentDashboardRepository.GetSADSizeLookupDataObjectAsync(string overallPhase, bool hasSixthForm, decimal noPupils, string term)
        {
            var container = _client.GetContainer(_databaseId, _sizeLookupCollectionId);

            var queryString = $"SELECT * FROM c WHERE " +
                $"c.OverallPhase=@OverallPhase and (is_null(c.HasSixthForm) or c.HasSixthForm=@HasSixthForm) " +
                $"and c.NoPupilsMin <= @NoPupils and (is_null(c.NoPupilsMax) or c.NoPupilsMax >= @NoPupils) " +
                $"and c.Term=@Term";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@OverallPhase", overallPhase)
                .WithParameter($"@HasSixthForm", hasSixthForm)
                .WithParameter($"@NoPupils", noPupils)
                .WithParameter($"@Term", term);

            var query = container.GetItemQueryIterator<SADSizeLookupDataObject>(queryDefinition, null);
            var result = (await query.ReadNextAsync()).FirstOrDefault();

            return result;
        }

        public async Task<List<SADSizeLookupDataObject>> GetSADSizeLookupListDataObject()
        {
            var container = _client.GetContainer(_databaseId, _sizeLookupCollectionId);

            var queryString = $"SELECT * FROM c";

            var queryDefinition = new QueryDefinition(queryString);

            var query = container.GetItemQueryIterator<SADSizeLookupDataObject>(queryDefinition, null);

            var results = new List<SADSizeLookupDataObject>();
            while (query.HasMoreResults)
            {
                FeedResponse<SADSizeLookupDataObject> response;
                try
                {
                    response = await query.ReadNextAsync();
                }
                catch (Exception)
                {
                    throw new ApplicationException("SAD Size lookup data could not be loaded!");
                }

                results.AddRange(response.ToList());
            }

            return results;
        }

        public async Task<SADFSMLookupDataObject> GetSADFSMLookupDataObjectAsync(string overallPhase, bool hasSixthForm, decimal fsm, string term)
        {
            var container = _client.GetContainer(_databaseId, _fsmLookupCollectionId);

            var queryString = $"SELECT * FROM c WHERE " +
                $"c.OverallPhase=@OverallPhase and (is_null(c.HasSixthForm) or c.HasSixthForm=@HasSixthForm) " +
                $"and c.FSMMin <= @FSM and c.FSMMax >= @FSM " +
                $"and c.Term=@Term";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@OverallPhase", overallPhase)
                .WithParameter($"@HasSixthForm", hasSixthForm)
                .WithParameter($"@FSM", fsm)
                .WithParameter($"@Term", term);

            var query = container.GetItemQueryIterator<SADFSMLookupDataObject>(queryDefinition, null);
            var result = (await query.ReadNextAsync()).FirstOrDefault();

            return result;
        }

        public async Task<List<SADFSMLookupDataObject>> GetSADFSMLookupListDataObject()
        {
            var container = _client.GetContainer(_databaseId, _fsmLookupCollectionId);

            var queryString = $"SELECT * FROM c";

            var queryDefinition = new QueryDefinition(queryString);

            var query = container.GetItemQueryIterator<SADFSMLookupDataObject>(queryDefinition, null);

            var results = new List<SADFSMLookupDataObject>();
            while (query.HasMoreResults)
            {
                FeedResponse<SADFSMLookupDataObject> response;
                try
                {
                    response = await query.ReadNextAsync();
                }
                catch (Exception)
                {
                    throw new ApplicationException("SAD FSM lookup data could not be loaded!");
                } 

                results.AddRange(response.ToList());
            }

            return results;
        }


        public async Task<List<SADSchoolRatingsDataObject>> GetSADSchoolRatingsDataObjectsAsync(string assesmentArea, string overallPhase, bool hasSixthForm, string londonWeighting, string size, string FSM, string term)
        {
            var container = _client.GetContainer(_databaseId, _sadCollectionId);

            var queryString = $"SELECT * FROM c WHERE " +
                $"c.AssessmentArea=@AssesmentArea " +
                $"and (is_null(c.OverallPhase) or c.OverallPhase=@OverallPhase) " +
                $"and (is_null(c.HasSixthForm) or c.HasSixthForm=@HasSixthForm) " +
                $"and (is_null(c.LondonWeighting) or contains(c.LondonWeighting, @LondonWeighting)) " +
                $"and (is_null(c.Size) or c.Size=@Size) " +
                $"and (is_null(c.FSM) or c.FSM=@FSM) " +
                $"and c.Term<=@Term";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@AssesmentArea", assesmentArea)
                .WithParameter($"@OverallPhase", overallPhase)
                .WithParameter($"@HasSixthForm", hasSixthForm)
                .WithParameter($"@Size", size)
                .WithParameter($"@FSM", FSM)
                .WithParameter($"@LondonWeighting", londonWeighting)
                .WithParameter($"@Term", term);

            var query = container.GetItemQueryIterator<SADSchoolRatingsDataObject>(queryDefinition, null);

            var results = new List<SADSchoolRatingsDataObject>();
            while (query.HasMoreResults)
            {
                FeedResponse<SADSchoolRatingsDataObject> response;
                try
                {
                    response = await query.ReadNextAsync();
                }
                catch (Exception)
                {
                    throw new ApplicationException("SAD data could not be loaded!");
                }

                results.AddRange(response.ToList());
            }

            return results;
        }
    }
}
