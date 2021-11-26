using Newtonsoft.Json;
using SFB.Web.ApplicationCore.Services;
using SFB.Web.ApplicationCore.Services.DataAccess;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SFB.Web.Infrastructure.Caching
{
    /// <summary>
    /// This class should be registered as singleton.    
    /// </summary>
    public class RedisCachedActiveEstablishmentIdsService : IActiveEstablishmentsService
    {
        private readonly IContextDataService _contextDataService;
        private readonly IFinancialDataService _financialDataService;
        private readonly ConnectionMultiplexer _connection;

        public RedisCachedActiveEstablishmentIdsService(IContextDataService contextDataService, IFinancialDataService financialDataService, string connectionString)
        {
            _contextDataService = contextDataService;
            _financialDataService = financialDataService;
            _connection = ConnectionMultiplexer.Connect(connectionString);

            //#if !DEBUG
            //   ClearCachedData();
            //#endif
        }

        //private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        //{
        //    string cacheConnection = ConfigurationManager.AppSettings["RedisConnectionString"].ToString();
        //    return ConnectionMultiplexer.Connect(cacheConnection);
        //});

        //public static ConnectionMultiplexer Connection
        //{
        //    get
        //    {
        //        return lazyConnection.Value;
        //    }
        //}

        public async Task<List<long>> GetAllActiveUrnsAsync()
        {
            var cache = _connection.GetDatabase();
            
            var serializedList = cache.StringGet("SFBActiveURNList");

            List<long> deserializedList;

            if (serializedList.IsNull)
            {
                deserializedList = await _contextDataService.GetAllSchoolUrnsAsync();

                cache.StringSet("SFBActiveURNList", JsonConvert.SerializeObject(deserializedList));
            }
            else
            {
                deserializedList = JsonConvert.DeserializeObject<List<long>>(serializedList);
            }
            
            return deserializedList;
        }

        public async Task<List<int>> GetAllActiveCompanyNosAsync()
        {
            var cache = _connection.GetDatabase();

            var serializedList = cache.StringGet("SFBActiveCompanyNoList");

            List<int> deserializedList;

            if (serializedList.IsNull)
            {
                deserializedList = await _financialDataService.GetAllTrustCompanyNosAsync();

                cache.StringSet("SFBActiveCompanyNoList", JsonConvert.SerializeObject(deserializedList));
            }
            else
            {
                deserializedList = JsonConvert.DeserializeObject<List<int>>(serializedList);
            }

            return deserializedList;
        }

        public async Task<List<long>> GetAllActiveFuidsAsync()
        {
            var cache = _connection.GetDatabase();

            var serializedList = cache.StringGet("SFBActiveFuidList");

            List<long> deserializedList;

            if (serializedList.IsNull)
            {
                deserializedList = await _contextDataService.GetAllFederationUidsAsync();

                cache.StringSet("SFBActiveFuidList", JsonConvert.SerializeObject(deserializedList));
            }
            else
            {
                deserializedList = JsonConvert.DeserializeObject<List<long>>(serializedList);
            }

            return deserializedList;
        }

        private void ClearCachedData()
        {
            var cache = _connection.GetDatabase();
            cache.KeyDelete("SFBActiveURNList");
            cache.KeyDelete("SFBActiveCompanyNoList");
            cache.KeyDelete("SFBActiveFuidList");
        }
    }
}