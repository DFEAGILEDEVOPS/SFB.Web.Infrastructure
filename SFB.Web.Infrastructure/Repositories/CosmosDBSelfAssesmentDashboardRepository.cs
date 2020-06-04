﻿using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.ApplicationCore.Helpers.Enums;
using SFB.Web.Infrastructure.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Repositories
{
    public class CosmosDBSelfAssesmentDashboardRepository : AppInsightsLoggable, ISelfAssesmentDashboardRepository
    {
        private readonly string _databaseId;
        private static CosmosClient _client;

        public CosmosDBSelfAssesmentDashboardRepository(ILogManager logManager) : base(logManager)
        {
            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = _databaseId = ConfigurationManager.AppSettings["database"];
        }

        public CosmosDBSelfAssesmentDashboardRepository(CosmosClient cosmosClient, string databaseId, ILogManager logManager) : base(logManager)
        {
            _client = cosmosClient;
            _databaseId = databaseId;
        }

        async Task<SADSizeLookupDataObject> ISelfAssesmentDashboardRepository.GetSADSizeLookupDataObjectAsync(string overallPhase, bool hasSixthForm, decimal noPupils, string term)
        {
            var container = _client.GetContainer(_databaseId, "SADSizeLookup");

            var queryString = $"SELECT * FROM c WHERE " +
                $"c.OverallPhase=@OverallPhase and (is_null(c.HasSixthForm) or c.HasSixthForm=@HasSixthForm) " +
                $"and c.NoPupilsMin <= @NoPupils and c.NoPupilsMax >= @NoPupils " +
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

        public async Task<SADFSMLookupDataObject> GetSADFSMLookupDataObjectAsync(string overallPhase, bool hasSixthForm, decimal fsm, string term)
        {
            var container = _client.GetContainer(_databaseId, "SADFSMLookup");

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

        public async Task<List<SADSchoolRatingsDataObject>> GetSADSchoolRatingsDataObjectsAsync(string assesmentArea, EstablishmentType financialType, string overallPhase, bool hasSixthForm, string londonWeighting, string size, string FSM, string term)
        {
            var container = _client.GetContainer(_databaseId, "SADSchoolRatingsPerAssessmentArea");

            var queryString = $"SELECT * FROM c WHERE " +
                $"c.AssessmentArea=@AssesmentArea " +
                $"and (is_null(c.OverallPhase) or c.OverallPhase=@OverallPhase) " +
                $"and (is_null(c.HasSixthForm) or c.HasSixthForm=@HasSixthForm) " +
                $"and (is_null(c.FinancialType) or c.FinancialType=@FinancialType) " +
                $"and (is_null(c.LondonWeighting) or contains(c.LondonWeighting, @LondonWeighting)) " +
                $"and (is_null(c.Size) or c.Size=@Size) " +
                $"and (is_null(c.FSM) or c.FSM=@FSM) " +
                $"and (is_null(c.Term) or c.Term=@Term)";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@AssesmentArea", assesmentArea)
                .WithParameter($"@OverallPhase", overallPhase)
                .WithParameter($"@HasSixthForm", hasSixthForm)
                .WithParameter($"@FinancialType", financialType.ToString())
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
                }catch(Exception ex)
                {
                    throw ex;
                }

                results.AddRange(response.ToList());
            }

            return results;
        }
    }
}