﻿using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Scripts;
using SFB.Web.ApplicationCore.DataAccess;
using SFB.Web.ApplicationCore.Entities;
using SFB.Web.ApplicationCore.Helpers.Constants;
using SFB.Web.Infrastructure.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SFB.Artifacts.Infrastructure.Helpers;

namespace SFB.Web.Infrastructure.Repositories
{
    // ReSharper disable once UnusedType.Global
    public class CosmosDbEdubaseRepository : AppInsightsLoggable, IEdubaseRepository
    {
        private readonly string _databaseId;
        private static CosmosClient _client;
        private readonly IDataCollectionManager _dataCollectionManager;

        public CosmosDbEdubaseRepository(IDataCollectionManager dataCollectionManager, ILogManager logManager) : base(logManager)
        {
            _dataCollectionManager = dataCollectionManager;

            var clientBuilder = new CosmosClientBuilder(ConfigurationManager.AppSettings["endpoint"], ConfigurationManager.AppSettings["authKey"]);

            _client = AppSettings.CosmosConnectionMode.Gateway.Equals(
                ConfigurationManager.AppSettings[AppSettings.CosmosConnectionMode.Key],
                StringComparison.OrdinalIgnoreCase)
                ? clientBuilder.WithConnectionModeGateway().Build()
                : clientBuilder.WithConnectionModeDirect().Build();

            _databaseId = ConfigurationManager.AppSettings["database"];

        }

        public CosmosDbEdubaseRepository(IDataCollectionManager dataCollectionManager, CosmosClient cosmosClient, string databaseId, ILogManager logManager) : base(logManager)
        {
            _dataCollectionManager = dataCollectionManager;

            _client = cosmosClient;

            _databaseId = databaseId;

        }

        public async Task<EdubaseDataObject> GetSchoolDataObjectByUrnAsync(long urn)
        {
            var schoolDataObjects = await GetSchoolDataObjectByIdAsync(new Dictionary<string, object> { { SchoolTrustFinanceDataFieldNames.URN, urn } });
            if(schoolDataObjects == null)
            {
                throw new ApplicationException("School data object could not be loaded from Edubase! URN:" + urn);
            }
            return schoolDataObjects.FirstOrDefault();
        }

        public async Task<List<EdubaseDataObject>> GetMultipleSchoolDataObjectsByUrnsAsync(List<long> urns)
        {
            return await GetMultipleSchoolDataObjectsByIdsAsync(SchoolTrustFinanceDataFieldNames.URN, urns);
        }

        public async Task<List<EdubaseDataObject>> GetSchoolsByLaEstabAsync(string laEstab, bool openOnly)
        {
            var parameters = new Dictionary<string, object>
            {
                {EdubaseDataFieldNames.LA_CODE, Int32.Parse(laEstab.Substring(0, 3))},
                {EdubaseDataFieldNames.ESTAB_NO, Int32.Parse(laEstab.Substring(3))}
            };

            if (openOnly)
            {
                parameters.Add(EdubaseDataFieldNames.ESTAB_STATUS, "Open");
            }

            return await GetSchoolDataObjectByIdAsync(parameters);
        }

        public async Task<List<long>> GetAllSchoolUrnsAsync()
        {
            var queryString = $"SELECT VALUE c.URN FROM c";

            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var queryDefinition = new QueryDefinition(queryString);
            var query = container.GetItemQueryIterator<long>();
            LogEvent("GetAllSchoolUrnsAsync", container.Id, queryDefinition);

            List<long> results = new List<long>();
            while (query.HasMoreResults)
            {
                var response = await query.ReadNextAsync();

                results.AddRange(response.ToList());
            }

            return results;
        }

        public async Task<List<long>> GetAllFederationUids()
        {
            var queryString = $"SELECT VALUE c['{EdubaseDataFieldNames.FEDERATION_UID}'] FROM c WHERE c['{EdubaseDataFieldNames.IS_FEDERATION}'] = true";

            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var queryDefinition = new QueryDefinition(queryString);
            var query = container.GetItemQueryIterator<long>();
            LogEvent("GetAllFederationUids", container.Id, queryDefinition);

            List<long> results = new List<long>();
            while (query.HasMoreResults)
            {
                var response = await query.ReadNextAsync();

                results.AddRange(response.ToList());
            }

            return results;
        }

        public async Task<IEnumerable<EdubaseDataObject>> GetAcademiesByCompanyNoAsync(int companyNo)
        {
            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var results = new List<EdubaseDataObject>();

            var queryString = $"SELECT c['{EdubaseDataFieldNames.URN}'], " +
                $"c['{EdubaseDataFieldNames.ESTAB_NAME}'], " +
                $"c['{EdubaseDataFieldNames.OVERALL_PHASE}'], " +
                $"c['{EdubaseDataFieldNames.COMPANY_NUMBER}'] " +
                $"FROM c WHERE c.{EdubaseDataFieldNames.COMPANY_NUMBER}=@CompanyNo " +
                $"AND c.{EdubaseDataFieldNames.FINANCE_TYPE} = 'Academies' " +
                $"AND c.{EdubaseDataFieldNames.ESTAB_STATUS_IN_YEAR} = 'Open'";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@CompanyNo", companyNo);
            LogEvent("GetAcademiesByCompanyNoAsync", container.Id, queryDefinition);

            try
            {
                var feedIterator = container.GetItemQueryIterator<EdubaseDataObject>(queryDefinition, null);

                while (feedIterator.HasMoreResults)
                {
                    foreach (var item in await feedIterator.ReadNextAsync())
                    {
                        results.Add(item);
                    }
                }

                return results;
            }
            catch (Exception ex)
            {
                var errorMessage = $"{collectionName} could not be loaded! : {ex.Message} : {queryDefinition.QueryText}";
                base.LogException(ex, errorMessage);
                return null;
            }
        }

        public async Task<IEnumerable<EdubaseDataObject>> GetAcademiesByUidAsync(int uid)
        {
            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var results = new List<EdubaseDataObject>();

            var queryString = $"SELECT c['{EdubaseDataFieldNames.URN}'], " +
                $"c['{EdubaseDataFieldNames.ESTAB_NAME}'], " +
                $"c['{EdubaseDataFieldNames.OVERALL_PHASE}'], " +
                $"c['{EdubaseDataFieldNames.COMPANY_NUMBER}'] " +
                $"FROM c WHERE c.{EdubaseDataFieldNames.UID}=@UID " +
                $"AND c.{EdubaseDataFieldNames.FINANCE_TYPE} = 'Academies'";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@UID", uid);
            LogEvent("GetAcademiesByUidAsync", container.Id, queryDefinition);

            try
            {
                var feedIterator = container.GetItemQueryIterator<EdubaseDataObject>(queryDefinition, null);

                while (feedIterator.HasMoreResults)
                {
                    foreach (var item in await feedIterator.ReadNextAsync())
                    {
                        results.Add(item);
                    }
                }

                return results;
            }
            catch (Exception ex)
            {
                var errorMessage = $"{collectionName} could not be loaded! : {ex.Message} : {queryDefinition.QueryText}";
                base.LogException(ex, errorMessage);
                return null;
            }
        }

        public async Task<int> GetAcademiesCountByCompanyNoAsync(int companyNo)
        {
            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var queryString = $"SELECT VALUE COUNT(c) " +
                $"FROM c WHERE c.{EdubaseDataFieldNames.COMPANY_NUMBER}=@CompanyNo " +
                $"AND c.{EdubaseDataFieldNames.FINANCE_TYPE} = 'Academies' " +
                $"AND c.{EdubaseDataFieldNames.ESTAB_STATUS_IN_YEAR} = 'Open'";

            var queryDefinition = new QueryDefinition(queryString)
                .WithParameter($"@CompanyNo", companyNo);
            LogEvent("GetAcademiesCountByCompanyNoAsync", container.Id, queryDefinition);

            try
            {
                var feedIterator = container.GetItemQueryIterator<int>(queryDefinition, null);
                return (await feedIterator.ReadNextAsync()).First();
            }
            catch (Exception ex)
            {
                var errorMessage = $"{collectionName} could not be loaded! : {ex.Message} : {queryDefinition.QueryText}";
                base.LogException(ex, errorMessage);
                return 0;
            }
        }

        #region Private methods

        private async Task<List<EdubaseDataObject>> GetSchoolDataObjectByIdAsync(Dictionary<string, object> fields)
        {
            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);

            var sb = new StringBuilder();
            foreach (var field in fields)
            {
                sb.Append($"c.{field.Key}=@{field.Key} AND ");
            }

            var where = sb.ToString().Substring(0, sb.ToString().Length - 5);

            var queryString =
                $"SELECT c['{EdubaseDataFieldNames.URN}'], c['{EdubaseDataFieldNames.UID}'], c['{EdubaseDataFieldNames.ESTAB_NAME}'], " +
                $"c['{EdubaseDataFieldNames.OVERALL_PHASE}'], c['{EdubaseDataFieldNames.PHASE_OF_EDUCATION}'], c['{EdubaseDataFieldNames.TYPE_OF_ESTAB}'], " +
                $"c['{EdubaseDataFieldNames.ADDRESS}'], c['{EdubaseDataFieldNames.LOCATION}'], " +
                $"c['{EdubaseDataFieldNames.GOV_OFFICE_REGION}'], c['{EdubaseDataFieldNames.ESTAB_STATUS}'], " +
                $"c['{EdubaseDataFieldNames.TRUSTS}'], c['{EdubaseDataFieldNames.COMPANY_NUMBER}'], " +
                $"c['{EdubaseDataFieldNames.LA_CODE}'], c['{EdubaseDataFieldNames.ESTAB_NO}'], c['{EdubaseDataFieldNames.TEL_NO}'], c['{EdubaseDataFieldNames.NO_PUPIL}'], " +
                $"c['{EdubaseDataFieldNames.STAT_LOW}'], c['{EdubaseDataFieldNames.STAT_HIGH}'], c['{EdubaseDataFieldNames.HEAD_FIRST_NAME}'], " +
                $"c['{EdubaseDataFieldNames.HEAD_LAST_NAME}'], c['{EdubaseDataFieldNames.MAT_SAT}'], c['{EdubaseDataFieldNames.SPONSORS}']," +
                $"c['{EdubaseDataFieldNames.NURSERY_PROVISION}'], c['{EdubaseDataFieldNames.OFFICIAL_6_FORM}'], c['{EdubaseDataFieldNames.SCHOOL_WEB_SITE}'], " +
                $"c['{EdubaseDataFieldNames.OFSTED_RATING}'], c['{EdubaseDataFieldNames.OFSTE_LAST_INSP}'], " +
                $"c['{EdubaseDataFieldNames.FINANCE_TYPE}'], c['{EdubaseDataFieldNames.IS_FEDERATION}'], c['{EdubaseDataFieldNames.IS_PART_OF_FEDERATION}'], " +
                $"c['{EdubaseDataFieldNames.FEDERATION_UID}'], c['{EdubaseDataFieldNames.FEDERATION_NAME}'], c['{EdubaseDataFieldNames.FEDERATION_MEMBERS}'], " +
                $"c['{EdubaseDataFieldNames.FEDERATIONS_CODE}'], c['{EdubaseDataFieldNames.FEDERATION}'], " +
                $"c['{EdubaseDataFieldNames.OPEN_DATE}'], " +
                $"c['{EdubaseDataFieldNames.CLOSE_DATE}'] FROM c WHERE {where}";

            var queryDefinition = new QueryDefinition(queryString);
            foreach (var field in fields)
            {
                queryDefinition = queryDefinition.WithParameter($"@{field.Key}", field.Value);
            }
            
            LogEvent("GetSchoolDataObjectByIdAsync", container.Id, queryDefinition);
            var results = new List<EdubaseDataObject>();

            try
            {
                var feedIterator = container.GetItemQueryIterator<EdubaseDataObject>(queryDefinition, null);

                while (feedIterator.HasMoreResults)
                {
                    foreach (var item in await feedIterator.ReadNextAsync())
                    {
                        results.Add(item);
                    }
                }

                if (results.Count == 0)
                {
                    throw new ApplicationException("School document not found in Edubase collection!");
                }

                return results;
            }
            catch (Exception ex)
            {
                var errorMessage = $"{collectionName} could not be loaded! : {ex.Message} : {queryDefinition.QueryText}";
                base.LogException(ex, errorMessage);
                return null;
            }        
        }

        private async Task<List<EdubaseDataObject>> GetMultipleSchoolDataObjectsByIdsAsync(string fieldName, List<long> ids)
        {
            var collectionName = await _dataCollectionManager.GetLatestActiveCollectionByDataGroupAsync(DataGroups.Edubase);

            var container = _client.GetContainer(_databaseId, collectionName);
            
            var queryString = $"SELECT c['{EdubaseDataFieldNames.URN}'], " +
                $"c['{EdubaseDataFieldNames.ESTAB_NAME}'], " +
                $"c['{EdubaseDataFieldNames.OVERALL_PHASE}'], " +
                $"c['{EdubaseDataFieldNames.TYPE_OF_ESTAB}'], " +
                $"c['{EdubaseDataFieldNames.ADDRESS}'], " +
                $"c['{EdubaseDataFieldNames.TEL_NO}'], " +
                $"c['{EdubaseDataFieldNames.HEAD_FIRST_NAME}'], " +
                $"c['{EdubaseDataFieldNames.HEAD_LAST_NAME}'], " +
                $"c['{EdubaseDataFieldNames.LA_CODE}'], " +
                $"c['{EdubaseDataFieldNames.NO_PUPIL}'], " +
                $"c['{EdubaseDataFieldNames.RELIGIOUS_CHARACTER}'], " +
                $"c['{EdubaseDataFieldNames.OFSTED_RATING}'], " +
                $"c['{EdubaseDataFieldNames.OFSTE_LAST_INSP}'], " +
                $"c['{EdubaseDataFieldNames.LOCATION}'], "+
                $"c['{EdubaseDataFieldNames.FINANCE_TYPE}'], c['{EdubaseDataFieldNames.IS_FEDERATION}'], c['{EdubaseDataFieldNames.IS_PART_OF_FEDERATION}'], " +
                $"c['{EdubaseDataFieldNames.FEDERATION_UID}'], c['{EdubaseDataFieldNames.FEDERATION_NAME}'], c['{EdubaseDataFieldNames.FEDERATION_MEMBERS}'], " +
                $"c['{EdubaseDataFieldNames.FEDERATIONS_CODE}'], c['{EdubaseDataFieldNames.FEDERATION}'] " +
                $"FROM c WHERE ARRAY_CONTAINS(@ids, c.{fieldName})";

            var results = new List<EdubaseDataObject>();
            try
            {
                var queryDefinition = new QueryDefinition(queryString)
                    .WithParameter("@ids", ids);
                
                var query = container.GetItemQueryIterator<EdubaseDataObject>(queryDefinition);
                LogEvent("GetMultipleSchoolDataObjectsByIdsAsync", container.Id, queryDefinition);

                while (query.HasMoreResults)
                {
                    var response = await query.ReadNextAsync();
                    results.AddRange(response.ToList());
                }
                
                if (results.Count < ids.Count)
                {
                    throw new JsonSerializationException();
                }
                
                return results;
            }
            catch (Exception ex)
            {
                var errorMessage = $"{collectionName} could not be loaded! : {ex.Message} : URNs = {string.Join(", ", ids)}";
                base.LogException(ex, errorMessage);
                throw new ApplicationException($"One or more documents could not be loaded from {collectionName} : URNs = {string.Join(", ", ids)}");
            }
        }
        
        private void LogEvent(string eventName, string containerId, QueryDefinition query)
        {
            LogEvent(eventName, new Dictionary<string, string>
            {
                { "Repository", nameof(CosmosDbEdubaseRepository) },
                { "Container.DatabaseId", _databaseId },
                { "Container.ContainerId", containerId },
                { "Container.Query", query.QueryText },
                { "Container.Params", JsonConvert.SerializeObject(query.GetQueryParameters()) }
            });
        }

        #endregion
    }
}
